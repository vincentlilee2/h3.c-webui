/* H3 WebUI v0.2: 秒数 1-20 + 交互模式 */
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

// 把"完成后 N 分钟"的人类可读格式(< 60 显示 X 分钟,< 2880 显示 X 小时,否则 X 天)
function formatCleanupAge(min) {
  if (min < 60) return `完成后 ${min} 分钟清理`;
  if (min < 24 * 60) return `完成后 ${(min / 60).toFixed(1).replace(/\.0$/, "")} 小时清理`;
  return `完成后 ${(min / (24 * 60)).toFixed(1).replace(/\.0$/, "")} 天清理`;
}

const form = $("#job-form");
const submitBtn = $("#submit-btn");
const statusText = $("#status-text");
const videoEl = $("#video");
const videoPlaceholder = $("#video-placeholder");
const logEl = $("#log");  // 实施日志板块已隐藏;保留引用以避免改动 SSE 处理逻辑
const phaseEl = $("#phase");
const elapsedEl = $("#elapsed");
const jobListEl = $("#job-list");
const currentJobIdEl = $("#current-job-id");
const currentJobStatusEl = $("#current-job-status");
const liveTraceEl = $("#live-trace");  // 右侧第二行:实时显示最近一行 log(代替原"范围 1-20s · 24 fps")
const jobParamsEl = $("#job-params");
const cancelBtn = $("#cancel-btn");
const queueInfoEl = $("#queue-info");
const cleanupInfoEl = $("#cleanup-info");
const barFillEl = $("#bar-fill");
const barEl = document.querySelector(".bar");
const healthSummaryEl = $("#health-summary");

let currentJob = null;
let eventSource = null;
let elapsedTimer = null;
let sessionId = null;             // 交互模式会话 ID
let sessionJobCount = 0;
let currentMode = "t2v";           // t2v | interactive

// ---------- Health ----------
async function checkHealth() {
  try {
    const r = await fetch("/api/health");
    const d = await r.json();
    if (d.ok && d.h3_exists && d.model_exists) {
      let txt = `✓ 就绪 · 范围 ${d.min_seconds}-${d.max_seconds}s · ${d.fps} fps`;
      if (d.current_running) txt += ` · 跑 ${d.current_running.slice(0,8)}`;
      if (d.queue_size > 0) txt += ` · 排队 ${d.queue_size}`;
      healthSummaryEl.textContent = txt;
      healthSummaryEl.style.color = "var(--ok)";
      cleanupInfoEl.textContent = formatCleanupAge(d.cleanup_after_minutes);
      queueInfoEl.textContent = d.queue_size > 0
        ? `${d.queue_size} 个任务排队中`
        : "无";
      // 保存 benchmarks 给预估用
      window.__h3_benchmarks = d.benchmarks || {};
      // 更新 cloud quota 显示
      updateCloudQuota(d);
    } else {
      healthSummaryEl.textContent = "✗ " + JSON.stringify(d);
      healthSummaryEl.style.color = "var(--err)";
    }
  } catch (e) {
    healthSummaryEl.textContent = "✗ 后端不可达: " + e.message;
    healthSummaryEl.style.color = "var(--err)";
  }
}

// ---------- 退出按钮:清理残留进程 ----------
$("#exit-btn").addEventListener("click", async () => {
  const ok = confirm(
    "退出 H3 WebUI?\n\n" +
    "后端服务器会被关闭,正在跑的本地任务会中断,云端任务会继续完成。\n" +
    "点确定 → 干净退出\n" +
    "点取消 → 啥都不做"
  );
  if (!ok) return;
  const btn = $("#exit-btn");
  btn.disabled = true;
  btn.textContent = "退出中...";
  try {
    const r = await fetch("/api/shutdown", { method: "POST" });
    const d = await r.json();
    if (r.ok) {
      // 服务器 0.3 秒后会自杀,这里给个明确提示
      document.body.innerHTML = `
        <div style="display:flex;align-items:center;justify-content:center;height:100vh;
                    flex-direction:column;font-family:system-ui;color:var(--fg);">
          <h1>✓ 后端已关闭</h1>
          <p style="color:var(--fg-dim)">PID ${d.pid} 已发送 SIGTERM</p>
          <p style="color:var(--fg-dim);margin-top:16px">浏览器标签可以关闭了</p>
        </div>`;
    } else {
      alert("退出失败: " + (d.detail || JSON.stringify(d)));
      btn.disabled = false;
      btn.textContent = "✕ 退出";
    }
  } catch (e) {
    alert("退出失败: " + e.message + "\n(后端可能已经死了 — 直接关浏览器即可)");
    btn.disabled = false;
    btn.textContent = "✕ 退出";
  }
});

// 根据 preset + seconds 预估耗时
function estimateSeconds(preset, seconds) {
  const bm = (window.__h3_benchmarks || {})[preset];
  if (!bm) return null;
  return Math.round(bm.seconds_per_video_second * seconds);
}
function formatEstimate(sec) {
  if (sec == null) return "—";
  if (sec < 60) return `~${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec - m * 60;
  return `~${m}m${s}s`;
}

// ---------- 加载交互命令列表 ----------
async function loadInteractiveCommands() {
  try {
    const r = await fetch("/api/interactive-commands");
    const d = await r.json();
    const list = $("#interactive-commands-list");
    list.innerHTML = d.commands.map(c =>
      `<div class="cmd-item"><code>${escapeHtml(c.cmd)}</code><span>${escapeHtml(c.desc)}</span></div>`
    ).join("");
  } catch (e) {
    console.error("interactive commands:", e);
  }
}

// ---------- 历史任务 ----------
async function refreshJobs() {
  try {
    let url = "/api/jobs?limit=6";  // 最多 6 条
    if (currentMode === "interactive" && sessionId) {
      url += "&session_id=" + encodeURIComponent(sessionId);
    }
    const r = await fetch(url);
    const d = await r.json();
    if (d.jobs.length === 0) {
      jobListEl.innerHTML = '<li class="empty">暂无任务</li>';
      return;
    }
    jobListEl.innerHTML = d.jobs.map(j => `
      <li data-job-id="${j.id}">
        <span class="job-id">${j.id}</span>
        <span class="job-prompt" title="${escapeAttr(j.prompt)}">${escapeHtml(j.prompt.slice(0, 36))}${j.prompt.length > 36 ? '…' : ''}</span>
        <span class="status-badge ${j.status}">${j.status}</span>
        <button type="button" class="job-delete" title="删除该记录" aria-label="删除 ${j.id}">✕</button>
      </li>
    `).join("");
    // 行点击 → 加载该任务
    jobListEl.querySelectorAll("li[data-job-id]").forEach(li => {
      li.addEventListener("click", (e) => {
        if (e.target.closest(".job-delete")) return;  // 点 ✕ 不触发加载
        loadJob(li.dataset.jobId);
      });
    });
    // ✕ 按钮 → confirm 删除
    jobListEl.querySelectorAll(".job-delete").forEach(btn => {
      btn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const li = btn.closest("li[data-job-id]");
        const id = li.dataset.jobId;
        if (!confirm(`确定删除记录 ${id} ?(不影响视频文件,仅从历史列表移除)`)) return;
        try {
          const r = await fetch(`/api/jobs/${encodeURIComponent(id)}`, { method: "DELETE" });
          if (!r.ok) throw new Error(await r.text());
          li.remove();  // 立即从 UI 移除,不必等下一次 refresh
          if (jobListEl.children.length === 0) {
            jobListEl.innerHTML = '<li class="empty">暂无任务</li>';
          }
        } catch (err) {
          alert(`删除失败: ${err.message || err}`);
        }
      });
    });
  } catch (e) {
    console.error("refresh jobs:", e);
  }
}

// ---------- Tabs 切换 ----------
$$(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    const mode = tab.dataset.mode;
    $$(".tab").forEach(t => t.classList.toggle("active", t === tab));
    currentMode = mode;
    $("#job-form").classList.toggle("hidden", mode !== "t2v");
    $("#interactive-panel").classList.toggle("hidden", mode !== "interactive");
    if (mode === "interactive" && !sessionId) {
      sessionId = "sess-" + Math.random().toString(36).slice(2, 8);
      sessionJobCount = 0;
      $("#session-id-display").textContent = sessionId;
    }
    refreshJobs();
  });
});

// ---------- 单次模式:秒数滑杆 ----------
const secondsRange = $("#seconds-range");
const secondsDisplay = $("#seconds-display");
const framesDisplay = $("#frames-display");
const presetSelect = $("#preset-select");
secondsRange.addEventListener("input", updateEstimate);
presetSelect.addEventListener("change", updateEstimate);

function updateEstimate() {
  const s = parseInt(secondsRange.value);
  secondsDisplay.textContent = s + " 秒";
  framesDisplay.textContent = s * 24;
  const est = estimateSeconds(presetSelect.value, s);
  if (est != null) {
    secondsDisplay.textContent = `${s} 秒 (预估 ${formatEstimate(est)})`;
  }
}

// ---------- 交互模式:秒数滑杆 ----------
const iSeconds = $("#interactive-seconds");
const iSecondsDisplay = $("#interactive-seconds-display");
const iPreset = $("#interactive-preset");
iSeconds.addEventListener("input", updateInteractiveEstimate);
iPreset.addEventListener("change", updateInteractiveEstimate);

function updateInteractiveEstimate() {
  const s = parseInt(iSeconds.value);
  const est = estimateSeconds(iPreset.value, s);
  iSecondsDisplay.textContent = est != null
    ? `${s} 秒 (预估 ${formatEstimate(est)})`
    : `${s} 秒`;
}

// ============ 引擎 pill 切换 + optgroup 过滤 + API key 弹窗 ============
const API_KEY_STORAGE = "h3webui_mmx_apikey";
let currentEngine = "local";
let pendingApiKeyFor = null;

function getStoredApiKey() {
  return localStorage.getItem(API_KEY_STORAGE) || null;
}
function setStoredApiKey(k) {
  localStorage.setItem(API_KEY_STORAGE, k);
}
function clearStoredApiKey() {
  localStorage.removeItem(API_KEY_STORAGE);
}

function filterSelectByEngine(selectEl, engine) {
  // <optgroup> 不响应 class="hidden" 隐藏,必须 style.display = "none"
  selectEl.querySelectorAll("optgroup").forEach(og => {
    if (og.dataset.engine === engine) {
      og.style.display = "";  // 默认
      og.disabled = false;
    } else {
      og.style.display = "none";  // 真隐藏
      og.disabled = true;
    }
  });
  // 选中第一个可见 optgroup 的第一项
  const visibleGroup = selectEl.querySelector(`optgroup[data-engine="${engine}"]`);
  if (visibleGroup) {
    const firstOption = visibleGroup.querySelector("option");
    if (firstOption) selectEl.value = firstOption.value;
  }
  selectEl.dispatchEvent(new Event("change"));
}

function showApiKeyModal(cloudPreset, onSuccess) {
  pendingApiKeyFor = cloudPreset;
  const modal = $("#api-key-modal");
  const title = $("#api-key-modal-title");
  const desc = $("#api-key-modal-desc");
  const input = $("#api-key-input");
  const status = $("#api-key-status");

  if (cloudPreset === "cloud_hailuo") {
    title.textContent = "配置 mmx API key (Hailuo-2.3)";
    desc.innerHTML = "用 mmx CLI 跑云端 Hailuo-2.3。TokenPlan key 每日 3 个免费视频配额。MiniMax-H3 需要 Pay-as-you-go key。";
  } else if (cloudPreset === "cloud_h3") {
    title.textContent = "配置 mmx API key (MiniMax-H3)";
    desc.innerHTML = "MiniMax-H3 需要 <strong>Pay-as-you-go</strong> 类型 API key。在 <a href='https://platform.minimaxi.com/user-center/basic-information/interface-key' target='_blank'>MiniMax 控制台</a> 创建并粘贴。";
  } else {
    title.textContent = "配置 API key";
    desc.textContent = "输入 mmx CLI 用的 API key。";
  }

  const existing = getStoredApiKey();
  input.value = existing || "";
  input.placeholder = "sk-cp-... 或 sk-...";
  status.textContent = existing ? "已有存储的 key,会覆盖" : "";
  status.className = "muted";

  // 已有 key 才显示「清除」按钮
  const clearBtn = $("#api-key-clear");
  if (clearBtn) {
    if (existing) clearBtn.classList.remove("hidden");
    else clearBtn.classList.add("hidden");
    clearBtn.onclick = () => {
      clearStoredApiKey();
      input.value = "";
      status.textContent = "✓ 已清除。下次会重新弹。";
      status.className = "ok";
      clearBtn.classList.add("hidden");
    };
  }

  modal.classList.remove("hidden");
  setTimeout(() => input.focus(), 50);

  $("#api-key-save").onclick = async () => {
    const k = input.value.trim();
    if (!k) {
      status.textContent = "key 不能为空";
      status.className = "err";
      return;
    }
    if (!k.startsWith("sk-")) {
      status.textContent = "key 格式看起来不对,通常以 'sk-' 开头";
      status.className = "err";
      return;
    }
    setStoredApiKey(k);
    status.textContent = "✓ 已保存,即将使用";
    status.className = "ok";
    setTimeout(() => {
      modal.classList.add("hidden");
      if (onSuccess) onSuccess(k);
    }, 500);
  };
  $("#api-key-cancel").onclick = () => {
    modal.classList.add("hidden");
    pendingApiKeyFor = null;
  };
}

// ============ 更新 cloud quota 显示(脚本顶层,不在回调内) ============
function updateCloudQuota(health) {
  const videoQ = health.cloud?.quota?.video;
  const hEl = document.getElementById("hailuo-quota");
  if (hEl) {
    if (videoQ) {
      hEl.textContent = `${videoQ.used}/${videoQ.total}`;
      if (videoQ.resets_at) {
        const d = new Date(videoQ.resets_at);
        hEl.title = `重置: ${d.toLocaleString()}`;
      }
    } else {
      hEl.textContent = "N/A";
    }
  }
  const h3El = document.getElementById("h3-status");
  if (h3El) {
    h3El.textContent = "需要 Pay-as-you-go key";
    h3El.title = "当前 TokenPlan key 不支持 H3";
  }
}

document.querySelectorAll("#engine-pills .pill").forEach(pill => {
  pill.addEventListener("click", () => {
    document.querySelectorAll("#engine-pills .pill").forEach(p => p.classList.remove("active"));
    pill.classList.add("active");
    const engine = pill.dataset.engine;
    currentEngine = engine;
    filterSelectByEngine(presetSelect, engine);
    // 云端:第一次点才弹,以后清掉则重新弹
    if (engine === "cloud") {
      const key = getStoredApiKey();
      if (!key) {
        showApiKeyModal();  // 不传 cloudPreset,弹窗通用
      } else {
        appendLog(`\n[已用本地存储的 mmx API key: ${key.slice(0,8)}...]\n`, "ok");
      }
    }
  });
});

// 初始化:默认显示 local 引擎
filterSelectByEngine(presetSelect, "local");

// ---------- 单次模式:首尾帧切换 ----------
$$('input[name="mode"]').forEach(radio => {
  radio.addEventListener("change", () => {
    const isFL = $('input[name="mode"]:checked').value === "firstlast";
    $("#frame-inputs").classList.toggle("hidden", !isFL);
  });
});

// ---------- 单次模式:提交 ----------
form.addEventListener("submit", async (e) => {
  e.preventDefault();
  submitBtn.disabled = true;
  statusText.textContent = "提交中...";

  const fd = new FormData(form);
  const mode = fd.get("mode");
  if (mode === "t2v") {
    fd.delete("first_frame");
    fd.delete("last_frame");
  }
  if (!fd.get("seed")) fd.delete("seed");

    // 根据 preset 值自动推断 engine:
    //   cloud_* → engine=cloud
    //   其他本地预设 → engine=local
    const presetVal = fd.get("preset");
    if (presetVal && presetVal.startsWith("cloud_")) {
      fd.set("engine", "cloud");
    } else {
      fd.set("engine", "local");
    }

  try {
    const r = await fetch("/api/jobs", { method: "POST", body: fd });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || "submit failed");
    const pos = d.queue_position;
    statusText.textContent = pos > 1
      ? `已提交,任务 ${d.job_id} (排队第 ${pos} 位)`
      : `已提交,任务 ${d.job_id}`;
    loadJob(d.job_id);
    refreshJobs();
    checkHealth();
  } catch (err) {
    statusText.textContent = "✗ " + err.message;
  } finally {
    submitBtn.disabled = false;
  }
});

// ---------- 交互模式:出片按钮 ----------
$("#interactive-submit").addEventListener("click", async () => {
  const prompt = $("#interactive-prompt").value.trim();
  if (!prompt) {
    $("#interactive-prompt").focus();
    return;
  }
  const preset = $("#interactive-preset").value;
  const seconds = parseInt(iSeconds.value);

  const fd = new FormData();
  fd.append("prompt", prompt);
  fd.append("preset", preset);
  fd.append("mode", "t2v");
  fd.append("seconds", seconds);
  fd.append("session_id", sessionId);

  // 根据 preset 推断 engine
  if (preset && preset.startsWith("cloud_")) {
    fd.set("engine", "cloud");
  } else {
    fd.set("engine", "local");
  }

  const btn = $("#interactive-submit");
  btn.disabled = true;
  btn.textContent = "提交中...";
  try {
    const r = await fetch("/api/jobs", { method: "POST", body: fd });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || "submit failed");
    sessionJobCount++;
    $("#session-job-count").textContent = sessionJobCount;
    loadJob(d.job_id);
    refreshJobs();
    checkHealth();
    $("#interactive-prompt").value = "";
  } catch (err) {
    alert("提交失败: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "▶ 出片";
  }
});

// 交互模式:Cmd+Enter 提交
$("#interactive-prompt").addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault();
    $("#interactive-submit").click();
  }
});

// 结束会话
$("#end-session-btn").addEventListener("click", async () => {
  if (!sessionId) return;
  try {
    await fetch(`/api/sessions/${encodeURIComponent(sessionId)}`, { method: "DELETE" });
  } catch (e) {}
  sessionId = null;
  sessionJobCount = 0;
  $("#session-id-display").textContent = "—";
  $("#session-job-count").textContent = "0";
  $$(".tab").forEach(t => t.classList.toggle("active", t.dataset.mode === "t2v"));
  $("#job-form").classList.remove("hidden");
  $("#interactive-panel").classList.add("hidden");
  currentMode = "t2v";
  refreshJobs();
});

// ---------- 加载并跟踪任务 ----------
async function loadJob(jobId) {
  if (eventSource) { eventSource.close(); eventSource = null; }
  stopElapsedTimer();

  currentJob = jobId;
  currentJobIdEl.textContent = jobId;
  if (logEl) logEl.textContent = "";
  videoEl.removeAttribute("src");
  videoPlaceholder.style.display = "";
  barFillEl.style.width = "0%";
  barEl.classList.add("indeterminate");
  cancelBtn.classList.remove("hidden");

  try {
    const r = await fetch(`/api/jobs/${jobId}`);
    if (!r.ok) throw new Error("not found");
    const job = await r.json();
    applyJobState(job);
    if (job.status === "queued" || job.status === "running") {
      subscribeSSE(jobId);
    }
  } catch (e) {
    if (logEl) logEl.textContent = "load failed: " + e.message;
  }
}

function applyJobState(job) {
  currentJobStatusEl.textContent = job.status;
  currentJobStatusEl.className = "status-badge " + job.status;
  jobParamsEl.textContent = formatJobParams(job);

  if (job.status === "queued") {
    elapsedEl.textContent = "0.0s";
    phaseEl.textContent = `排队中 (第 ${job.queue_position} 位)`;
    barEl.classList.add("indeterminate");
  } else if (job.status === "running") {
    // 从 job.predicted_total_sec 取(server 算好的)
    // 进度条由 startElapsedTimer tick() 用 elapsed/predicted 算真实百分比
    currentPredictedTotal = job.predicted_total_sec || 0;
    // phaseEl 显示"预估总用时" — 让用户一眼看出总耗时规模
    // (tick() 里再算"剩余 Xm" 显示在右侧 live-trace 区)
    phaseEl.textContent = currentPredictedTotal > 0
      ? `~${formatElapsed(currentPredictedTotal)}`
      : "—";
    startElapsedTimer(job.started_at);
    barEl.classList.add("indeterminate");  // 短暂 indeterminate 直到第一次 tick
  } else if (job.status === "done") {
    elapsedEl.textContent = formatElapsed(job.elapsed_sec);
    phaseEl.textContent = `${formatElapsed(job.elapsed_sec)} (实际)`;
    if (liveTraceEl) liveTraceEl.textContent = "✓ 完成";
    barEl.classList.remove("indeterminate");
    barFillEl.style.width = "100%";
    if (job.video_url) showVideo(job.video_url);
    cancelBtn.classList.add("hidden");
  } else if (job.status === "error") {
    if (logEl) logEl.textContent += `\n错误: ${job.error}\n`;
    phaseEl.textContent = "—";
    if (liveTraceEl) liveTraceEl.textContent = `✗ ${job.error ? job.error.slice(0, 50) : "错误"}`;
    barEl.classList.remove("indeterminate");
    cancelBtn.classList.add("hidden");
  } else if (job.status === "cancelled") {
    phaseEl.textContent = "已取消";
    if (liveTraceEl) liveTraceEl.textContent = "已取消";
    barEl.classList.remove("indeterminate");
    cancelBtn.classList.add("hidden");
  }
}

// 取消按钮
cancelBtn.addEventListener("click", async () => {
  if (!currentJob) return;
  cancelBtn.disabled = true;
  try {
    const r = await fetch(`/api/jobs/${currentJob}/cancel`, { method: "POST" });
    const d = await r.json();
    appendLog(`\n[${d.status}]\n`, "err");
    currentJobStatusEl.textContent = d.status;
    currentJobStatusEl.className = "status-badge " + d.status;
    cancelBtn.classList.add("hidden");
    stopElapsedTimer();
  } catch (e) {
    appendLog(`\n[取消失败: ${e.message}]\n`, "err");
  } finally {
    cancelBtn.disabled = false;
  }
});

function subscribeSSE(jobId) {
  eventSource = new EventSource(`/api/jobs/${jobId}/stream`);
  eventSource.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg.type === "log") {
      appendLog(msg.line);
      updatePhaseFromLog(msg.line);
    } else if (msg.type === "status") {
      // 状态切换(queued -> running)
      currentJobStatusEl.textContent = msg.status;
      currentJobStatusEl.className = "status-badge " + msg.status;
      if (msg.status === "running") {
        appendLog("\n[开始运行]\n", "meta");
        // 重新拉一次完整状态拿到 started_at
        fetch(`/api/jobs/${jobId}`).then(r => r.json()).then(job => {
          applyJobState(job);
        });
        checkHealth();
      }
    } else if (msg.type === "end") {
      fetch(`/api/jobs/${jobId}`).then(r => r.json()).then(job => {
        applyJobState(job);
        if (job.status === "done") appendLog("\n[完成 ✓]", "ok");
        else if (job.status === "error") appendLog("\n[出错 ✗]", "err");
        else if (job.status === "cancelled") appendLog("\n[已取消]", "err");
        stopElapsedTimer();
        eventSource.close();
        eventSource = null;
        refreshJobs();
        checkHealth();
      });
    }
  };
  eventSource.onerror = () => {
    appendLog("\n[SSE 连接断开]\n", "err");
  };
}

function appendLog(line, cls = "") {
  if (!logEl) return;  // 实施日志板块已隐藏
  const span = document.createElement("span");
  if (cls) span.className = cls;
  span.textContent = line + "\n";
  logEl.appendChild(span);
  logEl.scrollTop = logEl.scrollHeight;
}

// phaseEl 由 applyJobState("running") 设置为"预估生成用时"(例如 "~5m"),
// 这里不再每行 log 重置 — 让用户看到稳定的"还要等多久"信息。
function updatePhaseFromLog(line) {
  // 实时同步到右侧第二行 live-trace(代替原"范围 1-20s · 24 fps")
  if (liveTraceEl) {
    const cleaned = line.replace(/\s+/g, " ").trim();
    liveTraceEl.textContent = cleaned.length > 60 ? cleaned.slice(0, 60) + "…" : cleaned;
  }
}

function showVideo(url) {
  videoEl.src = url + "?t=" + Date.now();
  videoPlaceholder.style.display = "none";
  videoEl.load();
}

// 全局变量:当前任务的预测总时长(秒),由 applyJobState("running") 设,供 tick() 算进度
let currentPredictedTotal = 0;

function startElapsedTimer(startedAt) {
  if (elapsedTimer) clearInterval(elapsedTimer);
  function tick() {
    const sec = (Date.now() / 1000) - startedAt;
    elapsedEl.textContent = formatElapsed(sec);
    // 进度条按真实耗时比例 = elapsed / predicted_total_sec
    if (currentPredictedTotal > 0) {
      const pct = Math.min(100, (sec / currentPredictedTotal) * 100);
      barEl.classList.remove("indeterminate");
      barFillEl.style.width = pct.toFixed(1) + "%";
    }
  }
  tick();
  elapsedTimer = setInterval(tick, 200);
}
function stopElapsedTimer() {
  if (elapsedTimer) { clearInterval(elapsedTimer); elapsedTimer = null; }
}

function formatElapsed(sec) {
  if (sec == null) return "0.0s";
  sec = Math.max(0, sec);
  if (sec < 60) return sec.toFixed(1) + "s";
  const m = Math.floor(sec / 60);
  const s = (sec - m * 60).toFixed(1);
  return `${m}m${s}s`;
}

function formatJobParams(j) {
  // 优先用真实 h3/mmx 命令行解析(显示 steps/layers/reuse/int8/width/height/seed 等)
  if (j.cmd_args && Array.isArray(j.cmd_args) && j.cmd_args.length) {
    const parts = [];
    const arg = (k) => {
      const i = j.cmd_args.indexOf(k);
      return i >= 0 && i + 1 < j.cmd_args.length ? j.cmd_args[i + 1] : null;
    };
    // mode
    if (j.cmd_args.includes("--first-frame")) parts.push("firstlast");
    else parts.push(j.mode || "t2v");
    // seconds
    const sec = arg("--seconds");
    if (sec) parts.push(`${sec}s`);
    // 关键质量参数(本地 h3.c)
    const steps = arg("--steps");
    const layers = arg("--layers");
    const reuse = arg("--reuse");
    const width = arg("--width");
    const height = arg("--height");
    if (steps) parts.push(`steps=${steps}`);
    if (layers) parts.push(`layers=${layers}`);
    if (reuse) parts.push(`reuse=${reuse}`);
    if (width && height) parts.push(`${width}x${height}`);
    if (j.cmd_args.includes("--use-int8-row-fc2")) parts.push("int8");
    const seed = arg("--seed");
    if (seed != null) parts.push(`seed=${seed}`);
    // preset 名(便于识别)
    if (j.preset && !parts.includes(j.preset)) parts.push(j.preset);
    return parts.join(" · ");
  }
  // 兜底:disk-recovered 旧任务没有 cmd_args,显示 preset label 简化版
  const parts = [
    j.mode, `${j.seconds}s`, `${j.frames}f`, j.preset,
  ];
  if (j.seed != null) parts.push(`seed=${j.seed}`);
  if (j.session_id) parts.push(`session=${j.session_id.slice(0, 8)}`);
  return parts.join(" · ");
}

function escapeHtml(s) {
  return (s || "").replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}
function escapeAttr(s) { return escapeHtml(s); }

// ---------- init ----------
checkHealth();
loadInteractiveCommands();
refreshJobs();
setInterval(refreshJobs, 5000);
setInterval(checkHealth, 3000);  // 3 秒刷新一次健康/队列

// 默认初始 seconds=1
secondsDisplay.textContent = "1 秒";
framesDisplay.textContent = "24";
iSecondsDisplay.textContent = "1 秒";
updateEstimate();
updateInteractiveEstimate();
