"""
H3 WebUI - FastAPI 后端 (v0.5: cloud 任务进度可观测)

变更(v0.5):
  - cloud run_cloud_job 改用 --no-wait 立刻拿到 task_id,server 自行 mmx video task get 轮询
  - 状态变化(Preparing/Processing/Success)实时推到 SSE 订阅者,前端能看到阶段
  - Success 后用 mmx video download 拉文件(轮询-下载分离)

变更(v0.4):
  - UI 首屏适配(去掉顶部 brand / footer / 实施日志)+ 页面级 overflow:hidden
  - 清理策略 48h(H3_CLEANUP_MINUTES=2880)+ formatCleanupAge 自动换算
  - 启动时从 outputs/*.mp4 重建历史(restore_jobs_from_disk)
  - Job.cmd_args 字段 + formatJobParams 解析真实命令行参数
  - 默认 prompt 改小女孩+猫;新增 landscape_int8 预设

变更(v0.3):
  - GPU 互斥锁:同时只允许一个 h3 进程在跑,新任务进入 FIFO 队列
  - 自动清理:已完成的 outputs/logs 在 N 分钟后删除,JOBS 内存条目同步清理
  - 任务状态新增 queued(排队中)
  - SSE 推送 "queued" -> "running" 状态变化
"""

import asyncio
import json
import os
import shutil
import signal
import time
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------- 配置 ----------
# 全部路径从环境变量读,带合理默认(基于 antirez/h3.c 仓标准布局 ~/work/h3.c/)
# 如果你的 h3.c 仓不在 ~/work/h3.c/,设置 H3_BIN / H3_MODEL_DIR / MMX_BIN 覆盖
WEBUI_ROOT = Path(__file__).parent.resolve()  # 仓根目录,不再硬编码绝对路径
OUTPUTS_DIR = WEBUI_ROOT / "outputs"
UPLOADS_DIR = WEBUI_ROOT / "uploads"
LOGS_DIR = WEBUI_ROOT / "logs"
DATA_DIR = WEBUI_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
PREDICTIONS_LOG = DATA_DIR / "predictions.json"  # 持久化历史实测耗时
H3_BIN    = Path(os.environ.get("H3_BIN",    str(Path.home() / "work/h3.c/h3"))).expanduser().resolve()
MODEL_DIR = Path(os.environ.get("H3_MODEL_DIR", str(Path.home() / "work/h3.c/MiniMax-H3"))).expanduser().resolve()
MMX_BIN   = Path(os.environ.get("MMX_BIN",   str(Path.home() / ".local/bin/mmx"))).expanduser().resolve()  # mmx CLI 路径

OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

FPS = 24
MIN_SECONDS = 1
MAX_SECONDS = 20

# 清理策略:已完成任务多少分钟后删产物 + 内存条目(默认 48 小时 = 2880 分钟)
CLEANUP_AFTER_MINUTES = int(os.environ.get("H3_CLEANUP_MINUTES", "2880"))

PRESETS = {
    "fast": {
        "label": "Fast (4 steps, 512², SSD streaming)",
        "args": ["--steps", "4", "--layers", "50", "--reuse", "1",
                 "--ssd-streaming", "--width", "512", "--height", "512"],
    },
    "fast_int8": {
        # M5 专用:int8-row-fc2 走 Metal 4 TensorOps,denoise 加速 ~30%
        # 注意:不能与 ssd-streaming 同用(int8 需要 weights 全驻 GPU)
        # 内存峰值:DiT ~26GB,但 M5 32GB 内存 + unified memory 可以应付
        "label": "Fast+int8 (4 steps, 512², M5 TensorOps, ~30% 加速)",
        "args": ["--steps", "4", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "512", "--height", "512"],
    },
    "fast_int8_16x9": {
        # Fast+int8 的 16:9 横屏版:4 steps + int8 + 768x448
        # 像素 344K(比 fast_int8 512² 的 262K 多 31%),每段视频估时稍长但仍在 M5 32GB 内存内
        # 768=32×24, 448=32×14,h3.c 多 32 倍数约束满足
        "label": "Fast+int8 16:9 (4 steps, 768x448, M5 TensorOps)",
        "args": ["--steps", "4", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "768", "--height", "448"],
    },
    "fast_aggressive": {
        # 激进配置:reuse 2 + layers 45 + render 320 + int8
        # 注:--reuse 和 --core-reuse 不能同用;我们只用 --reuse 2
        # 5s 视频估时 2-3 分钟,质量中等
        # 内存峰值:weights 驻 GPU ~26GB(M5 32GB OK)
        "label": "Aggressive (reuse 2, layers 45, render 320, int8)",
        "args": ["--steps", "4", "--layers", "45", "--reuse", "2",
                 "--render-width", "320", "--render-height", "320",
                 "--use-int8-row-fc2",
                 "--width", "512", "--height", "512"],
    },
    "balanced": {
        "label": "Balanced (20 steps, 512², default)",
        "args": ["--steps", "20", "--layers", "50", "--reuse", "1",
                 "--ssd-streaming", "--width", "512", "--height", "512"],
    },
    "balanced_int8": {
        "label": "Balanced+int8 (20 steps, 512², M5 TensorOps)",
        "args": ["--steps", "20", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "512", "--height", "512"],
    },
    "quality_ssd": {
        # 50 步 + 768² + ssd-streaming: 32GB 机器唯一能跑的高质量路径
        # 注:int8 + ssd-streaming 互斥;此预设走 SSD streaming 不走 int8
        # 实测: 1秒视频 30min, 5秒视频 90+min(50 步 + 768² + 120帧)
        # 只适合愿意等几小时换顶级画质
        # 768² = 正方形,适合 IG / 小红书 / 抖音竖屏(裁切)
        "label": "Quality SSD (50 steps, 768² 正方形, SSD · 30min/1s, 90min/5s)",
        "args": ["--steps", "50", "--layers", "50", "--reuse", "1",
                 "--ssd-streaming", "--width", "768", "--height", "768"],
    },
    "quality_landscape_ssd": {
        # 30 步 + 1024x512 (2:1) + ssd-streaming: 手机横屏广适配
        # 像素数 524K(略少于 quality_ssd 590K)
        # 30 步 vs quality_ssd 50 步:denoise 时间减少 40%
        # 2:1 比例覆盖:
        #   - iPhone 13~16 全屏(屏幕 2.17:1,极小黑边)
        #   - 主流 Android 20:9 全屏(2.22:1)
        #   - 老 Android 18:9 完美填满
        #   - Sony Xperia 21:9 多些黑边(仍可接受)
        # 实测: 1秒视频 ~15min, 5秒视频 ~45min(30步 vs 50步的 0.6×)
        "label": "Quality Landscape (30 steps, 1024x512 2:1 手机横屏, SSD · ~15min/1s, ~45min/5s)",
        "args": ["--steps", "30", "--layers", "50", "--reuse", "1",
                 "--ssd-streaming", "--width", "1024", "--height", "512"],
    },
    # 已删除:quality_int8 — 在 32GB M5 上会 OOM(int8 + 768² + 50 步需 ~44GB,触发 swap 抖动)
    # 想用 50 步 768²:用 quality / quality_ssd(ssd-streaming 走 SSD 不驻 GPU)
    # 想用 int8 + 50 步:降级到 512²(目前没有这种预设,可加 quality_int8_512)
    "wide": {
        "label": "Wide (20 steps, 864x480, ~3-5min/1s, 15-20min/5s)",
        "args": ["--steps", "20", "--layers", "50", "--reuse", "1",
                 "--ssd-streaming", "--width", "864", "--height", "480"],
    },
    # 已删除:wide_int8_safe — 跟 landscape_int8 重复
    # wide_int8_safe (704x384) 跟 landscape_int8 (768x448) 都是 16:9 横屏 int8
    # landscape_int8 像素多 27% 但比例更接近 16:9 (12:7 vs 11:6),保留它
    # 想用更小像素的 16:9 int8:用 landscape_int8(等),不要再加 wide_int8_safe
    "portrait_int8": {
        # 9:16 竖屏手机比例, 448x768 (344K 像素,width=32*14,height=32*24)
        # h3.c 要求 width/height 必须是 32 的倍数
        # 像素量级接近 balanced_int8 (~262K),int8 应能跑
        # 用例:TikTok / Reels / 小红书 / 朋友圈短视频
        "label": "Portrait+int8 (20 steps, 448x768 9:16, 手机竖屏)",
        "args": ["--steps", "20", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "448", "--height", "768"],
    },
    "landscape_int8": {
        # 16:9 横屏 int8, 768x448 (344K 像素,跟 portrait_int8 同像素量)
        # 长边都=768,理论 patch attention 一致;竖屏同像素量 OK 推测横屏同 OK
        # 跟 wide_int8 (864x480=414K) 比更小,应在 OOM 临界点内
        # 用例:YouTube / 抖音横屏 / 微信朋友圈横屏
        "label": "Landscape+int8 (20 steps, 768x448 16:9, 横屏 int8)",
        "args": ["--steps", "20", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "768", "--height", "448"],
    },
    "portrait_int8_short": {
        # 9:16 竖屏 short 版:12 steps + 448x832 (略长比,适合快手/抖音完整竖屏 9:19.5)
        # 像素 372K (比 portrait_int8 448x768=344K 多 8%);12 步比 20 步快 ~40%
        # int8 路径: 加载在 M5 32GB 内存内(实测 ~15GB)
        # 用例: 短视频快速草稿 / 短故事叙述 / 多版本对比
        "label": "Portrait+int8 (12 steps, 448x832 short)",
        "args": ["--steps", "12", "--layers", "50", "--reuse", "1",
                 "--use-int8-row-fc2",
                 "--width", "448", "--height", "832"],
    },
    # ============ 云端 mmx 预设 (用 cloud engine, model 由 mmx 自动选) ============
    "cloud_hailuo": {
        # 占位预设 - 前端用 value="cloud_hailuo" 触发 cloud 引擎
        # 后端实际忽略 args (cloud 引擎用 mmx CLI 默认参数)
        # mmx 会按 mode (t2v/firstlast) 自动选 Hailuo-2.3 / Hailuo-02
        # 用户当前用 TokenPlan API key,够用
        "label": "☁️ Hailuo-2.3 (TokenPlan · 配额 0/3 已满,明天重置)",
        "args": [],  # cloud 引擎不需要 args
    },
    "cloud_h3": {
        # 占位预设 - 需要 Pay-as-you-go API key 才能跑
        # 当前 TokenPlan key 不支持 H3 (返回 2013 错误)
        # 等用户换 key 后可用
        "label": "☁️ MiniMax-H3 (需 Pay-as-you-go key · 当前 key 类型不支持)",
        "args": [],  # cloud 引擎不需要 args
    },
    "cloud_default": {
        # 内部占位,前端不应该显示
        "label": "Cloud Default (internal)",
        "args": [],
    },
}


# ---------- 实测历史(自适应预测) ----------
# 每次任务完成后,把 (preset, steps, layers, w, h, seconds, elapsed_sec) 持久化到
# data/predictions.json。下次同签名任务预测时,直接用历史实测 — 比公式/表查都准。
#
# 文件格式: JSON 列表,每条 {ts, preset, steps, layers, w, h, ssd, int8, seconds, elapsed_sec, cmd_args}
PREDICTIONS_LOG_MAX = 200  # 最多保留 200 条,超过的删最旧


def _load_predictions_log() -> list:
    """从磁盘加载历史实测列表;文件不存在/损坏返回空列表。"""
    try:
        if PREDICTIONS_LOG.exists():
            with PREDICTIONS_LOG.open() as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
    except Exception as e:
        print(f"[predictions] failed to load: {e}")
    return []


def _save_predictions_log(records: list):
    """持久化历史;保留最近 PREDICTIONS_LOG_MAX 条。"""
    try:
        # 按 ts 倒序,保留前 N 条
        records = sorted(records, key=lambda r: r.get("ts", 0), reverse=True)
        records = records[:PREDICTIONS_LOG_MAX]
        # 原子写:tmp + rename,避免崩溃半文件
        tmp = PREDICTIONS_LOG.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(records, f, indent=2)
        tmp.replace(PREDICTIONS_LOG)
    except Exception as e:
        print(f"[predictions] failed to save: {e}")


def _signature_key(preset: str, parsed: dict, seconds: int) -> tuple:
    """从 cmd_args 解析结果生成用于匹配历史记录的签名 key.

    完全相同参数的任务 = 同一签名,优先返回精确匹配。
    """
    return (
        preset,
        parsed.get("steps", 0),
        parsed.get("layers", 0),
        parsed.get("width", 0),
        parsed.get("height", 0),
        parsed.get("ssd_streaming", False),
        parsed.get("use_int8_row_fc2", False),
        seconds,
    )


def _record_actual_duration(cmd_args: list, preset: str, seconds: int, elapsed_sec: float):
    """任务完成后调用:持久化实测耗时。

    注:不依赖 cmd_args 解析成功 — 即便 preset 未知也记录(用原始 cmd_args)。
    """
    parsed = _parse_h3_args(cmd_args) or {}
    record = {
        "ts": time.time(),
        "preset": preset,
        "steps": parsed.get("steps", 0),
        "layers": parsed.get("layers", 0),
        "w": parsed.get("width", 0),
        "h": parsed.get("height", 0),
        "ssd": parsed.get("ssd_streaming", False),
        "int8": parsed.get("use_int8_row_fc2", False),
        "seconds": seconds,
        "elapsed_sec": round(elapsed_sec, 2),
    }
    records = _load_predictions_log()
    records.append(record)
    _save_predictions_log(records)


def _find_actual_duration(preset: str, parsed: dict, seconds: int) -> Optional[float]:
    """从历史找实测耗时。

    策略:
      1) 精确匹配 (preset, steps, layers, w, h, ssd, int8, seconds)
      2) 同 preset + 同 steps/layers/w/h 但不同 seconds → 按 sub-linear 缩放
      3) 同 preset 找 N 条最近似的做加权平均
    """
    records = _load_predictions_log()
    if not records:
        return None

    exact_key = _signature_key(preset, parsed, seconds)
    for r in records:
        rk = _signature_key(r["preset"], {
            "steps": r["steps"], "layers": r["layers"],
            "width": r["w"], "height": r["h"],
            "ssd_streaming": r["ssd"], "use_int8_row_fc2": r["int8"],
        }, r["seconds"])
        if rk == exact_key:
            return float(r["elapsed_sec"])

    # 部分匹配: 同 preset + 同 steps/layers/w/h(不管 seconds)
    partial = [
        r for r in records
        if r["preset"] == preset
        and r["steps"] == parsed.get("steps", 0)
        and r["layers"] == parsed.get("layers", 0)
        and r["w"] == parsed.get("width", 0)
        and r["h"] == parsed.get("height", 0)
        and r["ssd"] == parsed.get("ssd_streaming", False)
        and r["int8"] == parsed.get("use_int8_row_fc2", False)
    ]
    if partial:
        # 按 s/vs 线性缩放:实测_5s / 5 × 当前_seconds
        # 加权:最近的样本权重高
        partial.sort(key=lambda r: r["ts"], reverse=True)
        total_weight = 0
        weighted_sum = 0
        for i, r in enumerate(partial[:5]):  # 最多用 5 条
            weight = 1.0 / (i + 1)  # 最近的权重最高
            s_vs = r["elapsed_sec"] / r["seconds"]
            predicted = s_vs * seconds
            weighted_sum += predicted * weight
            total_weight += weight
        return weighted_sum / total_weight

    # 最粗匹配: 同 preset(无参数匹配)
    same_preset = [r for r in records if r["preset"] == preset]
    if same_preset:
        # 按 elapsed/seconds 比值加权平均
        same_preset.sort(key=lambda r: r["ts"], reverse=True)
        total_weight = 0
        weighted_sum = 0
        for i, r in enumerate(same_preset[:5]):
            weight = 1.0 / (i + 1)
            s_vs = r["elapsed_sec"] / r["seconds"]
            weighted_sum += s_vs * weight
            total_weight += weight
        avg_s_vs = weighted_sum / total_weight
        return avg_s_vs * seconds

    return None


# ---------- 任务总时长预测 ----------
# 用来前端按 elapsed / predicted 显示真实进度(而非按阶段硬编码的百分比)。
# 两层 fallback:
#   1) cmd_args 解析出 steps/layers/pixels/ssd/int8 → 公式预测(误差 ±30%)
#   2) benchmarks[preset].seconds_per_video_second × seconds → 表反查(误差 ±10%)
#   3) fallback: 76.4 s/vs(默认 fast)
#
# 公式推导见 references/prediction-formula.md
def predict_total_seconds(cmd_args: list, preset: str, seconds: int) -> float:
    """根据 cmd_args 预测任务总耗时(秒).

    cmd_args 是 server 在 run_h3_subprocess 之前记录的 h3 命令行(从 job.cmd_args 拿)。
    返回值用于前端 progress bar = elapsed / predicted * 100。

    优先级:
      0) 历史实测(同签名精确匹配或部分匹配) — 精度最高(±5%)
      1) benchmarks 表反查(对已知 preset,基于 5s 视频实测)
      2) 公式预测(cmd_args 解析)
      3) fallback: 默认 fast 速率
    """
    parsed = _parse_h3_args(cmd_args) or {}

    # 第 0 层: 历史实测(基于 data/predictions.json)
    actual = _find_actual_duration(preset, parsed, seconds)
    if actual is not None:
        return actual

    # 第 1 层: 用 benchmarks 表反查(对已知 preset 精度最高)
    bench = BENCHMARKS.get(preset)
    if bench:
        # s/vs 反推:
        #   base_total(s=5) = s/vs × 5 (实测基线)
        #   FIXED ≈ 200s (loading + silent + VAE + mux,基本不跟秒数变化)
        #   per_step = (s/vs × 5 - FIXED) / 5  (每秒视频对应 denoise 开销)
        #   1s 视频 per_step 大约 0.7x 5s 的(因为 24-frame batch 摊销少)
        sec = float(seconds)
        FIXED = 200
        if sec <= 5:
            per_step = (bench["seconds_per_video_second"] * 5 - FIXED) / 5
            scaling = max(0.7, (sec / 5) ** 0.5)  # 1s→0.7, 3s→0.77, 5s→1.0
            return FIXED + per_step * sec * scaling
        # >5s 视频按 linear
        return bench["seconds_per_video_second"] * sec

    # 第 2 层: 公式预测(对未实测的 preset,如 landscape_int8 / portrait_int8 / quality_*)
    if parsed:
        steps = parsed.get("steps", 4)
        layers = parsed.get("layers", 50)
        w = parsed.get("width", 512)
        h = parsed.get("height", 512)
        ssd = parsed.get("ssd_streaming", False)
        int8 = parsed.get("use_int8_row_fc2", False)
        pixel_norm = (w * h) / 262144.0

        # 固定开销(加载 + 静默区 + VAE + mux)
        load_s = 50 if ssd else 70
        if int8:
            load_s += 10
        silent_s = 70 if ssd else 50  # patch/final/shader 静默区
        vae_s = 30 * pixel_norm
        mux_s = 10

        # denoise: 每步 ~50s (校准到 balanced 20步/512²/SSD = 1000s/20 = 50 s/step)
        # 1s 视频的 denoise 时间比 5s 视频略多 (按 (s/5)^0.85 sub-linear 缩放)
        D_step = 50.0
        sec_factor = (seconds / 5.0) ** 0.85
        denoise_s = (D_step * steps * pixel_norm * (0.7 if int8 else 1.0)
                     * sec_factor)
        return load_s + silent_s + denoise_s + vae_s + mux_s

    # 第 3 层: fallback(默认 fast 速率)
    return 76.4 * float(seconds)


def _parse_h3_args(cmd_args: Optional[list]) -> Optional[dict]:
    """从 h3 命令行参数里抽出 steps/layers/width/height/ssd_streaming/use_int8_row_fc2."""
    if not cmd_args:
        return None
    result = {}
    i = 0
    while i < len(cmd_args):
        a = cmd_args[i]
        nxt = cmd_args[i + 1] if i + 1 < len(cmd_args) else None
        if a == "--steps" and nxt and nxt.lstrip("-").isdigit():
            result["steps"] = int(nxt)
            i += 2; continue
        if a == "--layers" and nxt and nxt.lstrip("-").isdigit():
            result["layers"] = int(nxt)
            i += 2; continue
        if a == "--width" and nxt and nxt.lstrip("-").isdigit():
            result["width"] = int(nxt)
            i += 2; continue
        if a == "--height" and nxt and nxt.lstrip("-").isdigit():
            result["height"] = int(nxt)
            i += 2; continue
        if a == "--ssd-streaming":
            result["ssd_streaming"] = True
        if a == "--use-int8-row-fc2":
            result["use_int8_row_fc2"] = True
        i += 1
    if "steps" in result and "width" in result and "height" in result:
        return result
    return None

INTERACTIVE_COMMANDS = [
    {"cmd": "!help", "desc": "列出所有交互命令"},
    {"cmd": "!status", "desc": "显示模型加载状态"},
    {"cmd": "!seed N", "desc": "设置随机种子 (random = 随机)"},
    {"cmd": "!seconds N", "desc": "设置视频秒数 (1-20)"},
    {"cmd": "!show on/off", "desc": "切换终端内预览帧"},
    {"cmd": "!save PATH", "desc": "保存当前帧到指定路径"},
    {"cmd": "!cache", "desc": "查看缓存命中"},
    {"cmd": "!first PATH", "desc": "设置首帧 (或 'clear')"},
    {"cmd": "!last PATH", "desc": "设置尾帧 (或 'clear')"},
    {"cmd": "!quit / !exit", "desc": "退出交互模式"},
]


class Job:
    def __init__(self, job_id: str):
        self.id = job_id
        self.engine: str = "local"    # local | cloud
        self.status = "queued"      # queued | running | done | error | cancelled
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.finished_at: Optional[float] = None
        self.elapsed_sec: Optional[float] = None
        self.prompt: str = ""
        self.preset: str = "fast"
        self.mode: str = "t2v"
        self.seconds: int = 1
        self.frames: int = 24
        self.seed: Optional[int] = None
        self.session_id: Optional[str] = None
        self.first_frame: Optional[str] = None
        self.last_frame: Optional[str] = None
        self.video_path: Optional[str] = None
        self.error: Optional[str] = None
        self.log_path: Path = LOGS_DIR / f"{job_id}.log"
        self.process: Optional[asyncio.subprocess.Process] = None
        self.log_subscribers: list[asyncio.Queue] = []
        self.queue_position: Optional[int] = None  # 在队列中的位置(从 1 开始)
        self.cmd_args: Optional[list] = None  # 真实拼装出的 h3 命令行(供前端展示具体参数)

    def to_dict(self):
        d = {
            "id": self.id,
            "engine": self.engine,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed_sec": self.elapsed_sec,
            "prompt": self.prompt,
            "preset": self.preset,
            "mode": self.mode,
            "seconds": self.seconds,
            "frames": self.frames,
            "seed": self.seed,
            "session_id": self.session_id,
            "first_frame": self.first_frame,
            "last_frame": self.last_frame,
            "video_path": self.video_path,
            "video_url": f"/api/jobs/{self.id}/video" if self.video_path else None,
            "error": self.error,
            "queue_position": self.queue_position,
            "cmd_args": self.cmd_args,
        }
        # running 时实时算 elapsed
        if self.status == "running" and self.started_at:
            d["elapsed_sec"] = time.time() - self.started_at
        # 预测总耗时(用于前端 progress bar 用 elapsed/predicted 算真实进度)
        # cloud 任务不预测(交给云端 mmx 控制);只对 local h3.c 任务
        if self.engine != "cloud" and self.cmd_args:
            d["predicted_total_sec"] = predict_total_seconds(
                self.cmd_args, self.preset or "fast", self.seconds or 5
            )
        return d


JOBS: dict[str, Job] = {}
SESSIONS: dict[str, list[str]] = {}

# ---------- GPU 互斥锁 ----------
# 单进程 asyncio 锁:同一时刻只有一个 h3 子进程在跑
# 队列管理:等待中的任务按 FIFO 顺序被 worker 取走
GPU_LOCK = asyncio.Lock()  # 本地 h3.c 任务互斥(本地 GPU 只一份)
CLOUD_LOCK = asyncio.Lock()  # 云端任务互斥(避免多 cloud 同时跑 IO/订阅串扰;通常 1 个就够)
LOCAL_QUEUE: asyncio.Queue = asyncio.Queue()  # 本地任务队列(FIFO)
CLOUD_QUEUE: asyncio.Queue = asyncio.Queue()  # 云端任务队列(FIFO)
LOCAL_WORKER_TASK: Optional[asyncio.Task] = None
CLOUD_WORKER_TASK: Optional[asyncio.Task] = None


# ---------- FastAPI ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时:恢复历史任务 + 启动 worker;关闭时:取消所有 worker task。"""
    global LOCAL_WORKER_TASK, CLOUD_WORKER_TASK
    # 启动时从磁盘 outputs/*.mp4 + logs/*.log 重建历史任务(原 prompt 已丢失,占位为 "(已恢复)")
    restore_jobs_from_disk()
    # 启动两个独立 worker:local 走 GPU_LOCK(本地 GPU 互斥),cloud 走 CLOUD_LOCK(网络 IO 互斥)
    # 两者并行 — cloud 不占本地 GPU,可以跟 local 同时跑
    LOCAL_WORKER_TASK = asyncio.create_task(local_worker())
    CLOUD_WORKER_TASK = asyncio.create_task(cloud_worker())
    # 启动定期清理任务
    asyncio.create_task(cleanup_loop())
    yield
    # 关闭时:取消 worker(避免 uvicorn 退出时后台 asyncio task 报 warning)
    for t in (LOCAL_WORKER_TASK, CLOUD_WORKER_TASK):
        if t and not t.done():
            t.cancel()


app = FastAPI(title="H3 WebUI", version="0.3.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def restore_jobs_from_disk():
    """启动时把 outputs/*.mp4 + logs/*.log 重新注册到 JOBS 字典(供历史列表显示)。"""
    if not OUTPUTS_DIR.exists():
        return
    out_files = sorted(OUTPUTS_DIR.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
    for mp4 in out_files:
        jid = mp4.stem
        if jid in JOBS:
            continue  # 已在内存里(说明 server 没真正重启过)
        log_path = LOGS_DIR / f"{jid}.log"
        mtime = mp4.stat().st_mtime
        job = Job(jid)
        job.status = "done"
        job.prompt = "(已恢复 · 原 prompt 未保存)"  # log 不含 prompt,只能占位
        job.created_at = mtime - 60
        job.started_at = mtime - 30
        job.finished_at = mtime
        job.video_path = str(mp4)
        job.log_path = log_path
        JOBS[jid] = job


async def local_worker():
    """本地 worker:从 LOCAL_QUEUE 取任务,用 GPU_LOCK 串行跑 h3.c 进程."""
    while True:
        try:
            job_id = await LOCAL_QUEUE.get()
        except asyncio.CancelledError:
            return

        job = JOBS.get(job_id)
        if not job:
            continue

        async with GPU_LOCK:
            job.status = "running"
            job.started_at = time.time()
            _update_queue_positions()

            for q in job.log_subscribers:
                try:
                    q.put_nowait({"type": "status", "status": "running"})
                except asyncio.QueueFull:
                    pass

            await run_h3_subprocess(job)

            job.finished_at = time.time()
            job.elapsed_sec = job.finished_at - job.started_at

            # 持久化实测耗时,下次同参数任务预测更准
            if job.status == "done" and job.engine != "cloud" and job.cmd_args:
                try:
                    _record_actual_duration(
                        job.cmd_args, job.preset or "fast",
                        job.seconds or 5, job.elapsed_sec,
                    )
                except Exception as e:
                    print(f"[record_actual] error: {e}")

            for q in job.log_subscribers:
                try:
                    q.put_nowait({
                        "type": "end",
                        "status": job.status,
                        "elapsed_sec": job.elapsed_sec,
                    })
                except asyncio.QueueFull:
                    pass

            _update_queue_positions()


async def cloud_worker():
    """云端 worker:从 CLOUD_QUEUE 取任务,用 CLOUD_LOCK 串行跑(避免多 cloud IO 撞车).

    与 local_worker 并行 — cloud 占远端 GPU,不跟 local 抢本地 GPU。
    """
    while True:
        try:
            job_id = await CLOUD_QUEUE.get()
        except asyncio.CancelledError:
            return

        job = JOBS.get(job_id)
        if not job:
            continue

        async with CLOUD_LOCK:
            job.status = "running"
            job.started_at = time.time()
            _update_queue_positions()

            for q in job.log_subscribers:
                try:
                    q.put_nowait({"type": "status", "status": "running"})
                except asyncio.QueueFull:
                    pass

            await run_cloud_job(job)

            job.finished_at = time.time()
            job.elapsed_sec = job.finished_at - job.started_at

            for q in job.log_subscribers:
                try:
                    q.put_nowait({
                        "type": "end",
                        "status": job.status,
                        "elapsed_sec": job.elapsed_sec,
                    })
                except asyncio.QueueFull:
                        pass

            _update_queue_positions()


# ---------- 实测基线 (M5, 独占 GPU) ----------
# seconds_per_video_second: 生成 1 秒视频耗时(秒)。
# 用于: (1) predict_total_seconds fallback, (2) /api/health 返回给前端显示
# 注意:不是 "每秒帧数",是 "每视频秒秒数"。例:fast 5s 视频 = 382s = 76.4 s/vs × 5
BENCHMARKS = {
    "fast":              {"seconds_per_video_second": 76.4,  "notes": "BF16 + SSD streaming, baseline"},
    "fast_int8":         {"seconds_per_video_second": 57.4,  "notes": "M5 TensorOps, ~25% 加速, 质量等效"},
    "fast_int8_16x9":    {"seconds_per_video_second": 74.6,  "notes": "M5 TensorOps + 768x448 16:9, 像素多 31% 故 ~30% 慢于 fast_int8"},
    "fast_aggressive":   {"seconds_per_video_second": 19.2,  "notes": "reuse 2 + render 320, ~4x 快, 简化场景 (v0.4 已移除)"},
    "balanced":          {"seconds_per_video_second": 240.2, "notes": "20 步 BF16, 高质量"},
    "balanced_int8":     {"seconds_per_video_second": 168.0, "notes": "20 步 + int8, ~30% 加速, 质量等效"},
    # 注:landscape_int8 / portrait_int8 / quality_ssd 等没列,因为参数变种多,直接用公式预测更准
}


@app.get("/api/health")
async def health():
    cloud = _cloud_quota_info()
    return {
        "ok": True,
        "h3_bin": str(H3_BIN),
        "h3_exists": H3_BIN.exists(),
        "model_dir": str(MODEL_DIR),
        "model_exists": MODEL_DIR.exists(),
        "mmx_bin": str(MMX_BIN),
        "mmx_exists": MMX_BIN.exists(),
        "fps": FPS,
        "min_seconds": MIN_SECONDS,
        "max_seconds": MAX_SECONDS,
        "cleanup_after_minutes": CLEANUP_AFTER_MINUTES,
        "queue_size": LOCAL_QUEUE.qsize() + CLOUD_QUEUE.qsize(),
        "current_running": _current_running_id(),
        "cloud": cloud,
        # 实测基线(M5, 独占 GPU),秒/video_seconds(不是秒/帧)
        "benchmarks": BENCHMARKS,
    }


def _current_running_id() -> Optional[str]:
    for j in JOBS.values():
        if j.status == "running":
            return j.id
    return None


def _cloud_quota_info() -> dict:
    """查 mmx CLI quota,失败则返回 unknown。"""
    if not MMX_BIN.exists():
        return {"available": False, "reason": "mmx CLI not found"}
    try:
        proc = __import__("subprocess").run(
            [str(MMX_BIN), "auth", "status", "--output", "json", "--quiet"],
            capture_output=True, text=True, timeout=10,
        )
        if proc.returncode != 0:
            return {"available": False, "reason": f"mmx exit {proc.returncode}"}
        data = json.loads(proc.stdout)
        quota = {}
        for m in data.get("usage", {}).get("model_remains", []):
            if m.get("model_name") in ("video", "general"):
                quota[m["model_name"]] = {
                    "used": m.get("current_interval_usage_count", 0),
                    "total": m.get("current_interval_total_count", 0),
                    "remaining_pct": m.get("current_interval_remaining_percent", 0),
                    "resets_at": m.get("end_time", 0),
                }
        return {
            "available": True,
            "method": data.get("method"),
            "quota": quota,
        }
    except Exception as e:
        return {"available": False, "reason": str(e)}


@app.post("/api/shutdown")
async def shutdown():
    """优雅关闭后端,用于清理残留进程。
    前端调用后,后端会先响应(让 fetch 拿到 200),再用 asyncio 在 0.3 秒后 SIGTERM 自己。
    """
    import asyncio as _asyncio
    pid = os.getpid()

    async def _die():
        await _asyncio.sleep(0.3)
        # 给前端 fetch 留出响应时间
        os.kill(pid, signal.SIGTERM)

    _asyncio.create_task(_die())
    return {"ok": True, "pid": pid, "message": "shutting down in 0.3s"}


@app.get("/api/engines")
async def engines():
    """列出可用引擎及其状态。"""
    cloud = _cloud_quota_info()
    return {
        "engines": [
            {
                "id": "local",
                "name": "Local (h3.c, Apple Silicon)",
                "description": "本地 M5 GPU,零成本,慢(4-20 分钟/5s),配额无限",
                "available": H3_BIN.exists() and MODEL_DIR.exists(),
                "presets": list(PRESETS.keys()),
                "max_seconds": MAX_SECONDS,
            },
            {
                "id": "cloud",
                "name": "Cloud (mmx H3)",
                "description": "云端 MiniMax H3,快(秒级-分钟级),有配额,2K 分辨率",
                "available": cloud.get("available", False),
                "quota": cloud.get("quota"),
                "presets": ["cloud_default"],
                "max_seconds": 15,
                "min_seconds": 4,
            },
        ],
    }


@app.get("/api/presets")
async def presets():
    return {"presets": PRESETS}


@app.get("/api/interactive-commands")
async def interactive_commands():
    return {"commands": INTERACTIVE_COMMANDS}


@app.post("/api/jobs")
async def create_job(
    prompt: str = Form(...),
    preset: str = Form("fast"),
    mode: str = Form("t2v"),
    seconds: int = Form(1),
    seed: Optional[int] = Form(None),
    session_id: Optional[str] = Form(None),
    engine: str = Form("local"),  # local | cloud
    first_frame: Optional[UploadFile] = File(None),
    last_frame: Optional[UploadFile] = File(None),
):
    if engine == "local":
        if not H3_BIN.exists():
            raise HTTPException(500, f"h3 binary missing: {H3_BIN}")
        if not MODEL_DIR.exists():
            raise HTTPException(500, f"model dir missing: {MODEL_DIR}")
        if preset not in PRESETS:
            # 提示:云端 preset(cloud_*) 应该走 engine=cloud 分支,别走 local
            hint = " (cloud preset should use engine='cloud', not 'local')" if preset.startswith("cloud_") else ""
            raise HTTPException(400, f"unknown preset: {preset}{hint}")
        seconds = max(MIN_SECONDS, min(MAX_SECONDS, seconds))
    elif engine == "cloud":
        if not MMX_BIN.exists():
            raise HTTPException(500, f"mmx CLI missing: {MMX_BIN}")
        # cloud preset 命名约定: cloud_<model>_<quality>_<mode>
        #   model:   hailuo (TokenPlan) | h3 (Pay-as-you-go)
        #   quality: std | fast   (Hailuo-2.3 标准档 vs Fast 档; H3 忽略 quality)
        #   mode:    t2v  | sef   (纯文/图; 首尾帧 — sef 会让 mmx 自动切到 Hailuo-02)
        # 历史值 "cloud_hailuo"/"cloud_h3"/"cloud_default" 仍兼容(当作 std+t2v)
        _LEGACY_CLOUD = {"cloud_hailuo", "cloud_h3", "cloud_default"}
        if preset not in _LEGACY_CLOUD and not (
            preset.startswith("cloud_") and preset.count("_") == 3
        ):
            raise HTTPException(400, f"unknown cloud preset: {preset}")
        # 兼容老 preset 值 → 统一映射到 cloud_hailuo_std_t2v / cloud_h3_std_t2v
        if preset == "cloud_hailuo":
            preset = "cloud_hailuo_std_t2v"
        elif preset == "cloud_h3":
            preset = "cloud_h3_std_t2v"
        elif preset == "cloud_default":
            preset = "cloud_hailuo_std_t2v"  # 历史 fallback
        # 云端: Hailuo-2.3 不接 --duration, H3 接 --duration 4-15
        # webui 让 mmx自己选模型,所以 seconds 限制放宽(Hailuo 默认 5s)
        seconds = max(1, min(15, seconds))  # 仍记录给 UI 显示
    else:
        raise HTTPException(400, f"unknown engine: {engine}")
    if mode not in ("t2v", "firstlast"):
        raise HTTPException(400, f"unknown mode: {mode}")
    if mode == "firstlast" and (not first_frame or not last_frame):
        raise HTTPException(400, "firstlast mode requires both first_frame and last_frame")

    job_id = uuid.uuid4().hex[:12]
    job = Job(job_id)
    job.prompt = prompt
    job.engine = engine
    job.preset = preset
    job.mode = mode
    job.seconds = seconds
    job.frames = seconds * FPS  # 注意:云端 FPS 不一定 24,这是显示用
    job.seed = seed
    job.session_id = session_id

    if first_frame:
        suffix = Path(first_frame.filename or "first.png").suffix or ".png"
        ff_path = UPLOADS_DIR / f"{job_id}_first{suffix}"
        with ff_path.open("wb") as f:
            shutil.copyfileobj(first_frame.file, f)
        job.first_frame = str(ff_path)
    if last_frame:
        suffix = Path(last_frame.filename or "last.png").suffix or ".png"
        lf_path = UPLOADS_DIR / f"{job_id}_last{suffix}"
        with lf_path.open("wb") as f:
            shutil.copyfileobj(last_frame.file, f)
        job.last_frame = str(lf_path)

    JOBS[job_id] = job
    if session_id:
        SESSIONS.setdefault(session_id, []).append(job_id)

    # 按 engine 分发到对应队列
    if job.engine == "cloud":
        await CLOUD_QUEUE.put(job_id)
    else:
        await LOCAL_QUEUE.put(job_id)
    _update_queue_positions()

    return {
        "job_id": job_id,
        "session_id": session_id,
        "queue_position": job.queue_position,
        "engine": engine,
    }


def _update_queue_positions():
    """按 FIFO 给所有 queued 任务分配位置(从 1 开始)。"""
    pos = 1
    for j in sorted(JOBS.values(), key=lambda x: x.created_at):
        if j.status == "queued":
            j.queue_position = pos
            pos += 1
        else:
            j.queue_position = None


async def run_cloud_job(job: Job):
    """通过 mmx CLI 提交云端视频任务,server 自己用 mmx video task get 轮询进度.

    流程:
      1. mmx video generate --no-wait 立刻拿到 task_id,mmx 进程立即退出(不再阻塞 30 分钟)
      2. server 用 mmx video task get 每 5 秒查一次状态
      3. 状态行通过 SSE 推到前端(progress bar / phase)
      4. Success 后用 mmx video download 拉文件到 outputs/
    """
    out_path = OUTPUTS_DIR / f"{job.id}.mp4"

    # 第一步: --no-wait 立刻提交并返回 task_id
    submit_cmd = [
        str(MMX_BIN), "video", "generate",
        "--prompt", job.prompt,
        "--no-wait",
        "--non-interactive",
        "--no-color",
    ]

    # 解析 cloud preset: cloud_<model>_<quality>_<mode>
    #   model:   hailuo | h3
    #   quality: std | fast   (Hailuo-2.3 标准/Fast 档; H3 忽略)
    #   mode:    t2v  | sef   (纯文/图; 首尾帧)
    # job.preset 已经被 /api/jobs 路由归一化(legacy → 新格式)
    _preset = job.preset  # e.g. "cloud_hailuo_std_t2v"
    _parts = _preset.split("_")
    if len(_parts) == 4:
        _, _model, _quality, _mode = _parts
    else:
        # 兜底:异常值(理论上不该到这里),按 hailuo_std_t2v 处理
        _model, _quality, _mode = "hailuo", "std", "t2v"

    # 显式锁模型 ID(避免 mmx 自动切到意外档位)
    if _model == "hailuo":
        if _quality == "fast":
            submit_cmd += ["--model", "MiniMax-Hailuo-2.3-Fast"]
        else:
            submit_cmd += ["--model", "MiniMax-Hailuo-2.3"]
    elif _model == "h3":
        # H3 忽略 quality 字段(只有 1 个档)
        submit_cmd += ["--model", "MiniMax-H3"]
    # 通用规则:只要用户上传了 first_frame,就走 I2V(传 --image)
    #   - t2v + std + image   → mmx 自动用 Hailuo-2.3-Fast(虽 preset 锁的是 std)
    #   - t2v + fast + image  → 显式 MiniMax-Hailuo-2.3-Fast
    #   - sef + image + last  → mmx 自动切到 Hailuo-02
    if job.first_frame and _mode == "sef" and job.last_frame:
        # 标准 SEF:首 + 尾帧都传
        submit_cmd += ["--image", job.first_frame, "--last-frame", job.last_frame]
    elif job.first_frame and _mode == "sef":
        # SEF 但只传了首帧:仍走 SEF(只传 image)
        submit_cmd += ["--image", job.first_frame]
    elif job.first_frame:
        # t2v + 有首帧:I2V(传 --image),是否切到 Fast 由 mmx 按 quality 决定
        submit_cmd += ["--image", job.first_frame]

    # 记录真实命令行(给前端展示)
    job.cmd_args = submit_cmd + ["(followed by mmx video task get polling)"]

    try:
        proc = await asyncio.create_subprocess_exec(
            *submit_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as e:
        job.status = "error"
        job.error = f"failed to start mmx: {e}"
        return

    submit_stdout = b""
    try:
        submit_stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:
        proc.kill()
        job.status = "error"
        job.error = "mmx submit timeout (>30s)"
        return

    if proc.returncode != 0:
        job.status = "error"
        job.error = f"mmx submit exit {proc.returncode}: {submit_stdout.decode(errors='replace')}"
        return

    # 解析 task_id from stdout: "[Model: MiniMax-Hailuo-2.3]\n{\"taskId\":\"...\"}"
    submit_text = submit_stdout.decode("utf-8", errors="replace")
    task_id = None
    for line in submit_text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                task_id = obj.get("taskId") or obj.get("task_id")
                if task_id:
                    break
            except json.JSONDecodeError:
                pass
    if not task_id:
        job.status = "error"
        job.error = f"mmx submit returned no task_id: {submit_text[:200]}"
        return

    # 把 task_id 写到 cmd_args(给前端 + 日志看)
    job.cmd_args = submit_cmd + [f"→ task_id={task_id}"]
    _emit_log(job, f"[cloud] task_id={task_id}")

    # 第二步: server 自己轮询
    await _poll_cloud_task(job, task_id, out_path)


async def _poll_cloud_task(job: Job, task_id: str, out_path: Path):
    """每 5 秒查一次云端任务状态,把进度推到 SSE 订阅者;Success 后下载 mp4."""
    POLL_INTERVAL = 5  # 秒
    TIMEOUT_SEC = 1800  # 30 分钟最大等待
    started = time.time()
    last_status = None

    while time.time() - started < TIMEOUT_SEC:
        # 推送心跳:让前端知道任务在轮询
        elapsed = int(time.time() - started)
        _emit_log(job, f"[cloud] poll t={elapsed}s status={last_status or '...'}")

        # mmx video task get --task-id X
        try:
            proc = await asyncio.create_subprocess_exec(
                str(MMX_BIN), "video", "task", "get",
                "--task-id", task_id,
                "--non-interactive", "--no-color",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        except (asyncio.TimeoutError, Exception) as e:
            _emit_log(job, f"[cloud] poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        try:
            obj = json.loads(stdout.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        status = obj.get("status", "")
        file_id = obj.get("file_id", "")

        # 状态变化时打一行日志(SSE 会推到前端)
        if status != last_status:
            _emit_log(job, f"[cloud] {status}" + (f" file_id={file_id}" if file_id else ""))
            last_status = status

        if status == "Success":
            if not file_id:
                job.status = "error"
                job.error = "Success but no file_id"
                return
            # 下载
            await _download_cloud_video(job, file_id, out_path)
            return
        elif status in ("Fail", "Failed"):
            job.status = "error"
            job.error = f"cloud failed: {obj}"
            return
        elif status in ("Preparing", "Processing", "Queueing", "Queued"):
            pass  # 正常轮询中

        await asyncio.sleep(POLL_INTERVAL)

    # 超时
    job.status = "error"
    job.error = f"cloud timeout after {TIMEOUT_SEC}s"


async def _download_cloud_video(job: Job, file_id: str, out_path: Path):
    """mmx video download 拉文件到 out_path,推到 SSE 进度。"""
    _emit_log(job, f"[cloud] downloading file_id={file_id}")
    try:
        proc = await asyncio.create_subprocess_exec(
            str(MMX_BIN), "video", "download",
            "--file-id", file_id,
            "--out", str(out_path),
            "--non-interactive", "--no-color",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # 边拉边推 stdout(有些 mmx 版本会打印进度)
        with job.log_path.open("a") as logf:
            async for line in proc.stdout:
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                logf.write(text + "\n")
                logf.flush()
                _emit_log(job, text)
        rc = await proc.wait()
    except Exception as e:
        job.status = "error"
        job.error = f"download failed: {e}"
        return

    if rc == 0 and out_path.exists() and out_path.stat().st_size > 0:
        job.video_path = str(out_path)
        job.status = "done"
        _emit_log(job, f"[cloud] saved {out_path.stat().st_size // 1024} KB")
    else:
        job.status = "error"
        job.error = f"download exit {rc}; file exists={out_path.exists()}"


def _emit_log(job: Job, line: str):
    """把一行写到 job.log_path + 推到所有 SSE 订阅者。"""
    with job.log_path.open("a") as logf:
        logf.write(line + "\n")
        logf.flush()
    for q in job.log_subscribers:
        try:
            q.put_nowait({"type": "log", "line": line})
        except asyncio.QueueFull:
            pass


async def run_h3_subprocess(job: Job):
    """启动 h3 子进程并实时转发 stdout 给 SSE 订阅者。"""
    out_path = OUTPUTS_DIR / f"{job.id}.mp4"
    preset = PRESETS[job.preset]

    cmd = [
        str(H3_BIN),
        "-d", str(MODEL_DIR),
        "-p", job.prompt,
        "--seconds", str(job.seconds),
        *preset["args"],
        "-o", str(out_path),
    ]
    if job.seed is not None:
        cmd += ["--seed", str(job.seed)]
    if job.mode == "firstlast":
        cmd += ["--first-frame", job.first_frame, "--last-frame", job.last_frame]
    # 记录真实命令行,供前端展示具体参数(steps/layers/reuse/int8/width/height)
    job.cmd_args = cmd

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(MODEL_DIR.parent),
        )
        job.process = proc
    except Exception as e:
        job.status = "error"
        job.error = f"failed to start h3: {e}"
        return

    with job.log_path.open("w") as logf:
        async for line in proc.stdout:
            text = line.decode("utf-8", errors="replace").rstrip("\n")
            logf.write(text + "\n")
            logf.flush()
            for q in job.log_subscribers:
                try:
                    q.put_nowait({"type": "log", "line": text})
                except asyncio.QueueFull:
                    pass
            if "wrote" in text and out_path.name in text:
                job.video_path = str(out_path)
                job.status = "done"

    rc = await proc.wait()
    if job.status == "running":
        if rc == 0 and out_path.exists():
            job.status = "done"
            job.video_path = str(out_path)
        else:
            job.status = "error"
            job.error = f"h3 exited with code {rc}"


async def cleanup_loop():
    """每分钟检查一次,清理已完成超过 N 分钟的任务产物。"""
    while True:
        try:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = []
            for job in JOBS.values():
                if job.status in ("done", "error", "cancelled"):
                    end_time = job.finished_at or job.created_at
                    age_min = (now - end_time) / 60
                    if age_min >= CLEANUP_AFTER_MINUTES:
                        to_delete.append(job)

            for job in to_delete:
                # 删 mp4
                if job.video_path:
                    Path(job.video_path).unlink(missing_ok=True)
                # 删 log
                job.log_path.unlink(missing_ok=True)
                # 删上传图片
                if job.first_frame:
                    Path(job.first_frame).unlink(missing_ok=True)
                if job.last_frame:
                    Path(job.last_frame).unlink(missing_ok=True)
                # 从 session 列表移除
                if job.session_id and job.session_id in SESSIONS:
                    SESSIONS[job.session_id] = [
                        jid for jid in SESSIONS[job.session_id] if jid != job.id
                    ]
                # 从内存移除
                del JOBS[job.id]

        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"cleanup error: {e}")


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """取消任务:running 杀进程,queued 从队列移除。"""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.status == "queued":
        # 标记为 cancelled;queue 里这个 id 会被 worker 取到时跳过
        job.status = "cancelled"
        _update_queue_positions()
        return {"ok": True, "status": "cancelled"}
    elif job.status == "running":
        if job.process:
            try:
                job.process.terminate()
            except ProcessLookupError:
                pass
            # 等最多 3 秒,超时就 SIGKILL(h3.c 在 Metal 计算中可能忽略 SIGTERM)
            try:
                await asyncio.wait_for(job.process.wait(), timeout=3.0)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    job.process.kill()  # SIGKILL
                    await asyncio.wait_for(job.process.wait(), timeout=2.0)
                except (asyncio.TimeoutError, ProcessLookupError):
                    pass
        job.status = "cancelled"
        return {"ok": True, "status": "cancelled"}
    else:
        raise HTTPException(400, f"job already {job.status}")


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job.to_dict()


@app.get("/api/jobs")
async def list_jobs(session_id: Optional[str] = None, limit: int = 50):
    items = sorted(JOBS.values(), key=lambda j: j.created_at, reverse=True)
    if session_id:
        ids = set(SESSIONS.get(session_id, []))
        items = [j for j in items if j.id in ids]
    return {"jobs": [j.to_dict() for j in items[:limit]]}


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """从历史列表移除一条记录;不删视频文件(防止误删正在用的产物)"""
    job = JOBS.pop(job_id, None)
    if not job:
        raise HTTPException(404, "job not found")
    # 从任何会话引用里也移除
    for sid, ids in SESSIONS.items():
        if job_id in ids:
            ids.remove(job_id)
    return {"ok": True, "id": job_id}


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    SESSIONS.pop(session_id, None)
    return {"ok": True}


@app.get("/api/jobs/{job_id}/video")
async def get_video(job_id: str):
    job = JOBS.get(job_id)
    if not job or not job.video_path:
        raise HTTPException(404, "video not ready")
    p = Path(job.video_path)
    if not p.exists():
        raise HTTPException(404, "video file missing")
    return FileResponse(p, media_type="video/mp4", filename=p.name)


@app.get("/api/jobs/{job_id}/log")
async def get_log(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if not job.log_path.exists():
        return {"lines": []}
    return {"lines": job.log_path.read_text().splitlines()}


@app.get("/api/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    job.log_subscribers.append(queue)

    async def event_gen():
        try:
            # 先回放已记录的日志
            if job.log_path.exists():
                for line in job.log_path.read_text().splitlines():
                    yield f"data: {json.dumps({'type': 'log', 'line': line})}\n\n"

            # 然后实时推送
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    if job.status in ("done", "error", "cancelled"):
                        break
                    continue
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") == "end":
                    break
        finally:
            try:
                job.log_subscribers.remove(queue)
            except ValueError:
                pass

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


app.mount("/static", StaticFiles(directory=str(WEBUI_ROOT / "static")), name="static")


@app.get("/")
async def index():
    return FileResponse(WEBUI_ROOT / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("H3_PORT", "8765")), log_level="info")
