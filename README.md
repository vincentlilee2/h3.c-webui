# H3 WebUI

Apple Silicon (M5) 本地 H3 视频生成的 Web 界面,基于 [antirez/h3.c](https://github.com/antirez/h3.c)(MiniMax-H3 推理引擎)。

可选接入 [MiniMax 平台](https://platform.minimaxi.com) 的 mmx CLI,使用云端 Hailuo-2.3 / MiniMax-H3 引擎(走 TokenPlan 或 Pay-as-you-go 配额)。

## 功能

- **文生视频 (text → video)**: 输入 prompt, 选择预设, 一键生成 MP4
- **首尾帧控制 (first/last frame)**: 上传两张图片作为起止帧
- **云端引擎** (可选): mmx CLI 跑 Hailuo-2.3 (1080p / 768p) 或 MiniMax-H3 (Pay-as-you-go)
- **实时进度**: SSE 推送 h3 子进程的 stdout, 前端展示阶段(tokenizer / text encoder / AdaLN / transformer load / denoise / video VAE / FFmpeg)
- **实时耗时**: 前端 200ms 刷新一次 elapsed
- **进度条**: 按实际耗时/预估总时比例推进(v0.6+ 自适应历史校准,v0.7+ 按本次任务的 steps/layers/分辨率/ssd/int8 8 维签名匹配历史记录)
- **本地/云端双引擎并行**: 本地 h3.c 和云端 mmx 各有独立 worker 队列,互不阻塞(v0.5.1)
- **预设**: 11 个本地预设 (fast / fast_int8 / fast_int8_16x9 / balanced / balanced_int8 / quality_ssd / quality_landscape_ssd / wide / portrait_int8 / portrait_int8_short / landscape_int8) + 5 个云端预设
- **历史任务**: 列出最近 50 个任务, 启动时从 outputs/*.mp4 自动重建(只保留时间戳/状态/参数,不保留 prompt)
- **退出按钮**: 后端支持 `/api/shutdown` 一键关闭(v0.5+)

## 启动

```bash
cd ~/work/h3-webui  # 或你 clone 的路径
pip install fastapi uvicorn python-multipart   # 依赖见 "依赖" 节
python3 server.py
# 然后浏览器打开 http://127.0.0.1:8765
```

## 依赖

```bash
pip install fastapi 'uvicorn[standard]' python-multipart pydantic
```

## 路径配置(全部从环境变量读,带合理默认)

| 资源 | 环境变量 | 默认值 |
|---|---|---|
| h3 二进制 | `H3_BIN` | `~/work/h3.c/h3` |
| 模型目录 | `H3_MODEL_DIR` | `~/work/h3.c/MiniMax-H3` |
| mmx CLI(云端可选) | `MMX_BIN` | `~/.local/bin/mmx` |
| 清理间隔 | `H3_CLEANUP_MINUTES` | `2880` (48 小时) |
| 监听端口 | `H3_PORT` | `8765` |

如果你的 h3.c 仓不在 `~/work/h3.c/`,在启动前 export:

```bash
export H3_BIN=/path/to/your/h3.c/h3
export H3_MODEL_DIR=/path/to/your/MiniMax-H3
python3 server.py
```

## 架构

```
Browser (HTML/CSS/JS)
    │ POST /api/jobs (multipart: prompt + preset + mode + 可选 first/last frame)
    │ GET  /api/jobs/{id}/stream (SSE: 日志 + 状态)
    │ GET  /api/jobs/{id}/video (MP4)
    ▼
FastAPI (server.py)
    │ LOCAL_QUEUE + CLOUD_QUEUE 双 worker 队列
    │ 创建 asyncio.subprocess → h3 二进制
    │ 读 stdout → 写 job.log + 推 SSE 订阅者
    ▼
h3 binary (默认 ~/work/h3.c/h3)
    │ FL2VA checkpoint (默认 ~/work/h3.c/MiniMax-H3/FL2VA/)
    │ Metal 4 on Apple Silicon GPU
    ▼
outputs/<job_id>.mp4
```

云端任务走 `mmx video generate --no-wait` → server 自行轮询 `mmx video task get`,Success 后 `mmx video download` 拉文件。

## API

| Method | Path | 用途 |
|---|---|---|
| GET | `/api/health` | 检查 h3 二进制 + 模型目录 + mmx CLI 是否就绪 |
| GET | `/api/presets` | 列出预设 + 元数据(label/args) |
| POST | `/api/jobs` | 创建任务, multipart: `prompt`, `preset`, `mode`, `first_frame?`, `last_frame?`, `engine=local\|cloud` |
| GET | `/api/jobs` | 列出最近 50 个任务(`?limit=N` 可调) |
| DELETE | `/api/jobs/{id}` | 从历史列表移除(不删视频文件) |
| GET | `/api/jobs/{id}` | 单个任务状态 |
| GET | `/api/jobs/{id}/stream` | SSE 实时日志 + 完成事件 |
| GET | `/api/jobs/{id}/video` | 下载生成的 MP4 |
| GET | `/api/jobs/{id}/log` | 读取完整日志(已结束任务) |
| POST | `/api/shutdown` | 一键关闭后端(SIGTERM 自己) |
| GET | `/` | 前端 SPA |
| GET | `/static/*` | CSS / JS |

## 关键设计

- **内存任务表**: `JOBS: dict[str, Job]`,重启后从 `outputs/*.mp4` 自动重建(只保留时间戳/状态,prompt 不可恢复——h3 log 不含 -p 参数)
- **SSE 实时推送**: 每个 job 维护 `log_subscribers: list[Queue]`,h3 子进程 stdout 异步推送
- **断点续传友好**: 任务列表持久化在 `outputs/` 文件系统上(按 job_id 命名)
- **安全**: 只监听 `127.0.0.1`,无鉴权(本地用)。云端 mmx API key 仅存在浏览器 `localStorage["h3webui_mmx_apikey"]`,不进任何后端文件
- **上传图片**: 限制文件类型 image/*,文件名用 job_id 前缀避免冲突
- **GPU 互斥**: `GPU_LOCK`(本地)+ `CLOUD_LOCK`(云端),两个队列互不阻塞

## 已知限制

- 多用户并发: 当前 `_subscribers` 全局共享,多浏览器同时看同一 job 没问题,但同时跑多个本地 job 会抢 GPU(h3 占用显存 9-25 GiB)
- 32 GB 内存机器: 预设默认都加 `--ssd-streaming`,峰值约 9.5 GiB DiT 临时 + 9.5 GiB VAE 峰值;int8 模式峰值更高(~15 GiB)
- first/last frame 上传: 要求两张同尺寸/相近内容的图(h3 会做拉伸+裁剪)
- 重启后历史 prompt 不可恢复(只有 video 时间戳/状态/参数);视频文件本身保留在 `outputs/`

## License

MIT
