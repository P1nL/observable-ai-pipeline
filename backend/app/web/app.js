/* A read-through view of persisted execution, not a browser-side scheduler. */
"use strict";
(() => {
  const $ = (id) => document.getElementById(id);
  const names = {normalize_asset: "输入素材预处理", segment_subject: "主体分割", analyze_motion: "运动与镜头规划", generate_frames: "图生视频生成", encode_package: "编码与封装", publish_result: "结果归档", mock: "单步推理验证"};
  const managedNames = {prepare_demo: "素材预处理", render_demo: "内容生成"};
  // Presentation profiles only: never written into frozen commands or used for routing.
  const demoModels = {segment_subject: "SAM 2", generate_frames: "Wan2.1-I2V"};
  const stepDescriptions = {
    normalize_asset: "图像解码、尺寸归一化与输入检查", segment_subject: "提取主体区域与前景掩膜",
    analyze_motion: "分析运动条件与镜头变化", generate_frames: "根据输入素材与运动条件生成帧序列",
    encode_package: "组织生成帧并输出预览文件", publish_result: "汇总输出文件并提交可下载结果",
    mock: "验证单步执行与结果提交", prepare_demo: "准备下游生成所需的输入资产", render_demo: "读取上游资产并提交生成结果",
  };
  const cleanupNames = {CPU_ONLY: "CPU 执行（无 GPU 槽位）", CPU_PROCESS_UNCONFIRMED: "进程退出待核实", QUARANTINED: "资源已隔离", RELEASED: "资源已释放", RESERVED: "资源已预留", RUNNING: "资源使用中"};
  const eventNames = {
    TASK_SUBMITTED: "任务已提交", STEP_REGISTERED: "已添加工作流步骤", WORKFLOW_CLOSED: "工作流登记已结束",
    WAITING_STEPS: "当前步骤已完成，等待业务继续登记", ATTEMPT_STARTED: "开始执行", EXECUTOR_STARTED: "执行进程已启动",
    EXECUTION_COMMAND_PERSISTED: "执行指令已提交", EXTERNAL_COMMAND_OBSERVED: "执行服务已确认接收指令",
    CHECKPOINT_COMMITTED: "生成进度已保存，可用于中断恢复", STEP_SUCCEEDED: "步骤已完成", STEP_CACHED: "已复用已有资产，跳过本次计算",
    ATTEMPT_FAILED: "本次执行失败", ATTEMPT_LOST: "执行失联，原进程退出待核实", ATTEMPT_TIMED_OUT: "本次执行超时", ATTEMPT_CANCELED: "本次执行已取消",
    TASK_SUCCEEDED: "任务已完成，生成结果可查看", TASK_FAILED: "任务失败，等待处理", TASK_RESUMED: "任务已恢复，保留已完成步骤与检查点",
    TASK_CANCEL_REQUESTED: "已请求取消，等待执行退出与清理", TASK_CANCELED: "任务已取消", STEP_CANCELED: "步骤已取消", TASK_DEADLINE: "已到达任务截止时间",
    RESOURCE_WAITING: "等待可用算力", RESOURCE_RESERVED: "执行槽位已预留", RESOURCE_RUNNING: "执行槽位使用中",
    RESOURCE_RELEASED: "执行槽位已释放", RESOURCE_QUARANTINED: "执行槽位已隔离，等待退出确认", CPU_EXECUTOR_CLEANED: "CPU 执行进程已清理",
    NODE_WAITING: "等待可用执行节点", STEP_DISPATCH_REJECTED: "步骤分配被拒绝", EXECUTOR_UNAVAILABLE: "执行服务暂不可达", SOFT_WARNING: "执行时长提醒",
  };
  const statuses = {QUEUED: "已排队", PENDING: "待执行", READY: "待领取", RUNNING: "运行中", SUCCEEDED: "已成功", CACHED: "缓存命中", FAILED: "已失败", LOST: "已失联", TIMED_OUT: "已超时", RETRY_WAIT: "等待重试", BLOCKED: "被阻塞", CANCELED: "已取消", CANCELING: "取消中", AVAILABLE: "可用", RESERVED: "已预留", QUARANTINED: "已隔离", ONLINE: "在线", OFFLINE: "离线", STALE: "观察过期", UNKNOWN: "未知"};
  const waits = {WAITING_STEPS: "等待追加工作流步骤", WAITING_CLEANUP: "等待退出确认", WAITING_EXECUTOR: "等待执行服务", EXECUTOR_UNAVAILABLE: "执行服务暂不可达", INVALID_CONFIG: "执行配置不兼容", WAITING_GPU: "等待 GPU 执行槽位", WAITING_CPU: "等待 CPU 并发配额", RESOURCE_CLEANUP: "等待执行器清理", PROCESS_EXIT_PENDING: "等待进程退出确认", RETRY_BACKOFF: "等待自动重试", ARTIFACT_UNAVAILABLE: "已提交资产不可用", UPSTREAM_FAILED: "上游步骤失败"};
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const terminal = new Set(["SUCCEEDED", "FAILED", "CANCELED"]);
  const pending = new Map();
  let taskId = "", generation = 0, timer, controller, actionController, busy = false, actionBusy = false;
  let snapshot = null, history = {}, events = new Map(), cursor = 0, rendered = {};
  let activeStep = "", followStep = true, detailTab = "flow", stepSignature = "", focusDetail = false;
  let detailTarget = null;
  let monitorTimer, monitorController, monitorBusy = false, monitorRevision = 0, monitorData = null, queueSignature = "", resourceSignature = "", attentionSignature = "";
  const queue = {status: "", q: "", offset: 0, limit: 10};
  let monitorState = "loading", detailState = "idle", detailMessage = "", monitorObserved = "";
  let acknowledgementBusy = false;

  function el(tag, text, className) {
    const result = document.createElement(tag);
    if (text !== undefined && text !== null) result.textContent = String(text);
    if (className) result.className = className;
    return result;
  }
  function date(value) {
    return value ? new Date(value).toLocaleString("zh-CN", {hour12: false}) : "—";
  }
  function badge(status) {
    const result = el("span", statuses[status] || status, "status");
    result.dataset.status = status;
    result.title = status;
    return result;
  }
  function pair(label, value, mono = false) {
    const result = el("div", null, "detail-pair");
    result.append(el("span", label, "muted"), el("span", value ?? "—", mono ? "mono" : ""));
    return result;
  }
  function stepName(task, key) {
    const labels = task.pipeline_id === "managed-v1" ? managedNames : names;
    return Object.hasOwn(labels, key) ? labels[key] : key;
  }
  function taskName(task) {
    const labels = {"managed-v1": "自定义生成任务", "animation-dag-v2": "图生视频工作流", "animation-v1": "动画生成 · 历史三步", "smoke-v1": "单步推理验证"};
    return Object.hasOwn(labels, task.pipeline_id) ? labels[task.pipeline_id] : task.pipeline_id;
  }
  function nodeName(id) {
    return id === "node-a" ? "推理节点 A" : id === "node-b" ? "推理节点 B" : id === "legacy" ? "历史执行环境" : id;
  }
  function stepModel(task, key) {
    return task.pipeline_id !== "managed-v1" && Object.hasOwn(demoModels, key) ? demoModels[key] : "";
  }
  function elapsed(start, end) {
    if (!start) return "未开始";
    const seconds = Math.max(0, Math.floor(((end ? new Date(end).getTime() : Date.now()) - new Date(start).getTime()) / 1000));
    if (!Number.isFinite(seconds)) return "—";
    return seconds < 60 ? `${seconds} 秒` : seconds < 3600 ? `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒` : `${Math.floor(seconds / 3600)} 小时 ${Math.floor(seconds % 3600 / 60)} 分`;
  }
  function renderTaskBrief(task) {
    const config = task.config_snapshot || {}, request = config.request || {}, input = request.input || {}, asset = config.input_asset;
    const inputView = el("div", null, "task-input"), inputFacts = el("div", null, "brief-input-facts");
    inputFacts.append(el("h3", "输入素材"));
    if (asset?.id) {
      const image = el("img", null, "input-thumbnail"), unavailable = el("p", "输入预览暂不可用", "muted"); unavailable.hidden = true;
      image.alt = "任务输入素材"; image.decoding = "async";
      image.addEventListener("error", () => { image.hidden = true; unavailable.hidden = false; });
      image.src = `/api/v1/inputs/${encodeURIComponent(asset.id)}/content`;
      inputView.append(image, unavailable);
      inputFacts.append(el("p", `${asset.media_type || "图像"} · ${asset.width} × ${asset.height}`, "muted"));
      const link = el("a", "查看原图", "download"); link.href = image.src; link.target = "_blank"; link.rel = "noopener";
      inputFacts.append(link);
    } else inputFacts.append(el("p", input.kind === "synthetic" ? "合成素材" : input.kind === "uploaded" ? "上传素材" : "由业务步骤提供输入", "muted"));
    inputView.append(inputFacts);
    const facts = el("div", null, "brief-facts"), models = task.steps.map((step) => stepModel(task, step.step_key)).filter(Boolean);
    if (models.length) facts.append(pair("参考模型（演示）", [...new Set(models)].join(" / ")));
    else facts.append(pair("执行方式", task.pipeline_id === "managed-v1" ? "按业务登记顺序执行" : "单步执行"));
    if (input.seed != null) facts.append(pair("随机种子", input.seed, true));
    const generation = config.steps?.find((step) => step.key === "generate_frames")?.parameters;
    if (Number.isInteger(generation?.batch_count) && Number.isInteger(generation?.frames_per_batch)) facts.append(pair("目标帧数", `${generation.batch_count * generation.frames_per_batch} 帧`));
    if (request.cache_enabled !== undefined) facts.append(pair("结果复用", request.bypass_cache ? "本次强制重新生成" : request.cache_enabled ? "允许复用已提交结果" : "关闭缓存复用"));
    const backend = config.execution_backend?.kind || request.execution_backend;
    if (backend) facts.append(pair("执行后端", backend === "external" ? "独立执行服务" : "本地执行服务"));
    $("task-brief").replaceChildren(inputView, facts);
  }
  function isOpenManaged(task) {
    return task.pipeline_id === "managed-v1" && task.registration_closed === false;
  }
  function registrationLabel(task) {
    if (task.pipeline_id !== "managed-v1") return "";
    if (task.registration_closed) return "登记已结束";
    return terminal.has(task.status) || task.status === "CANCELING" ? "登记未关闭" : "流程未结束";
  }
  function registeredSummary(registered, completed) {
    return `已登记 ${registered} 步 / 已完成 ${completed} 步`;
  }
  function executionLink(id, label, step = "", attempt = "") {
    const link = el("a", label, "execution-link"), query = new URLSearchParams();
    if (step) query.set("step", step);
    if (attempt) query.set("attempt", attempt);
    link.href = `/tasks/${encodeURIComponent(id)}${query.size ? `?${query}` : ""}`;
    link.dataset.executionTask = id; link.dataset.executionStep = step; link.dataset.executionAttempt = attempt;
    link.title = [id, step, attempt].filter(Boolean).join(" / ");
    return link;
  }
  function revealExecution() {
    if (!detailTarget || !snapshot) return;
    let target;
    if (detailTarget.attempt) {
      selectTab("attempts");
      target = $("attempts").querySelector(`[data-attempt="${CSS.escape(detailTarget.attempt)}"]`);
      if (target) target.tabIndex = -1;
    } else if (detailTarget.step && snapshot.steps.some((step) => step.step_key === detailTarget.step)) {
      selectTab("flow"); activeStep = detailTarget.step; followStep = false; renderSteps(snapshot);
      target = $("steps").querySelector(`[data-step="${CSS.escape(activeStep)}"] .step-select`);
    }
    if (target) {
      target.focus({preventScroll: true}); target.scrollIntoView({block: "nearest"});
      detailTarget = null; focusDetail = false;
    }
  }
  function connection(state, text) {
    detailState = state; detailMessage = text; renderConnection();
  }
  function renderConnection() {
    const offline = monitorState === "offline" || detailState === "offline";
    const online = monitorState === "online" && (!taskId || detailState === "online");
    const text = offline ? (monitorState === "offline" ? "监控连接中断" : "任务连接中断") : online ? "已连接" : "连接中";
    $("connection").dataset.state = offline ? "offline" : online ? "online" : "loading";
    if ($("connection").textContent !== text) $("connection").textContent = text;
    $("connection").title = [monitorObserved && `队列同步于 ${monitorObserved}`, detailMessage].filter(Boolean).join(" · ");
  }
  function selectTab(name, focus = false) {
    detailTab = name;
    for (const button of document.querySelectorAll("[data-detail-tab]")) {
      const selected = button.dataset.detailTab === name;
      button.setAttribute("aria-selected", String(selected));
      button.tabIndex = selected ? 0 : -1;
      $(button.getAttribute("aria-controls")).hidden = !selected;
      if (selected && focus) button.focus();
    }
    if (name === "flow") requestAnimationFrame(drawEdges);
  }

  function chooseTask(id, {replace = false, scroll = false} = {}) {
    if (id === taskId) {
      if (scroll) $("task-content").scrollIntoView({behavior: "smooth", block: "start"});
      return;
    }
    window.history[replace ? "replaceState" : "pushState"]({}, "", `/tasks/${id}`);
    focusDetail = scroll && matchMedia("(max-width: 900px)").matches;
    load(id);
  }

  function renderAttention(data) {
    const count = data.unacknowledged_count ?? data.unconfirmed_count;
    const pendingItems = data.unacknowledged_executors ?? data.unconfirmed_executors ?? [];
    const known = data.acknowledged_executors || [], knownCount = data.acknowledged_count || 0;
    const signature = JSON.stringify([count, pendingItems, knownCount, known, acknowledgementBusy]);
    if (signature === attentionSignature) return;
    const rows = [], wasOpen = $("resource-attention").querySelector("details")?.open;
    $("resource-warning").hidden = !count;
    $("resource-warning").textContent = count ? `${count} 项待核实` : "";
    function entry(item, acknowledged) {
      const row = el("div", null, "attention-entry");
      const link = executionLink(item.task_id, `${item.step_key} · ${item.attempt_id.slice(0, 8)} · 查看尝试记录`, item.step_key, item.attempt_id);
      link.className += " resource-attention-link"; link.title += ` / ${item.resource_cleanup_state}`;
      row.append(link);
      // Older API responses remain visible but cannot offer unsupported writes.
      if (data.unacknowledged_count !== undefined) {
        const button = el("button", acknowledged ? "恢复提醒" : "确认知悉", "button-secondary");
        button.type = "button"; button.dataset.acknowledgement = item.attempt_id;
        button.dataset.restore = String(acknowledged); button.disabled = acknowledgementBusy;
        row.append(button);
      }
      if (acknowledged) row.append(el("p", `知悉于 ${date(item.acknowledged_at)} · 仍未确认退出`, "muted"));
      return row;
    }
    if (count) {
      rows.push(el("p", `${count} 个执行器未确认退出，资源未释放。`, "resource-alert"));
      rows.push(el("p", "确认知悉仅移出提醒，不确认退出或释放资源。", "muted"));
      rows.push(...pendingItems.map((item) => entry(item, false)));
      if (count > pendingItems.length) rows.push(el("p", "展示最近 5 个待核实执行器。", "muted"));
    }
    if (knownCount) {
      const details = el("details", null, "acknowledged-attention"); details.dataset.detail = "acknowledged-attention";
      details.open = Boolean(wasOpen);
      details.append(el("summary", `已知悉记录 · ${knownCount} 项`), el("p", "已移出顶部提醒；退出状态仍未核实，资源占用保持不变。", "muted"), ...known.map((item) => entry(item, true)));
      if (knownCount > known.length) details.append(el("p", "展示最近 5 条已知悉记录。", "muted"));
      rows.push(details);
    }
    $("resource-attention").replaceChildren(...rows);
    attentionSignature = signature;
  }
  async function changeAcknowledgement(attemptId, restore) {
    if (acknowledgementBusy || !monitorData) return;
    const item = (restore ? monitorData.acknowledged_executors : monitorData.unacknowledged_executors)?.find((entry) => entry.attempt_id === attemptId);
    if (!item) return;
    acknowledgementBusy = true; renderAttention(monitorData);
    const message = $("resource-action-message"), controller = new AbortController();
    const deadline = setTimeout(() => controller.abort(), 8000);
    message.textContent = restore ? "正在恢复提醒…" : "正在保存知悉状态…";
    try {
      await request(`/api/v1/monitor/attempts/${encodeURIComponent(attemptId)}/acknowledgement`, controller.signal,
        restore ? {method: "DELETE"} : {method: "POST", headers: {"Content-Type": "application/json"},
          body: JSON.stringify({status: item.status, resource_cleanup_state: item.resource_cleanup_state, stop_requested_at: item.stop_requested_at})});
      message.textContent = restore ? "已恢复提醒，执行状态与资源占用未改变。" : "已确认知悉，执行状态与资源占用未改变。";
    } catch (error) {
      message.textContent = error.code === "MONITOR_CONDITION_CHANGED" ? "告警状态已变化，请刷新后重新确认。" : "操作结果未确认，请稍后重试；不会据此隐藏提醒或释放资源。";
    } finally {
      clearTimeout(deadline); acknowledgementBusy = false; attentionSignature = "";
      renderAttention(monitorData); restartMonitor();
    }
  }
  function renderQueue(data) {
    const counts = data.counts;
    $("stat-total").textContent = data.total;
    $("stat-running").textContent = counts.RUNNING;
    $("stat-succeeded").textContent = counts.SUCCEEDED;
    $("stat-failed").textContent = counts.FAILED;
    $("queue-page").textContent = `${Math.floor(queue.offset / queue.limit) + 1} / ${Math.max(1, Math.ceil(data.filtered_total / queue.limit))}`;
    $("queue-prev").disabled = queue.offset === 0;
    $("queue-next").disabled = queue.offset + queue.limit >= data.filtered_total;
    renderAttention(data);
    const signature = JSON.stringify([data.items, taskId]);
    if (signature !== queueSignature) {
      const focused = document.activeElement?.closest("[data-task]")?.dataset.task;
      const rows = data.items.map((task) => {
        const row = el("button", null, "queue-item");
        row.type = "button"; row.dataset.task = task.id; row.dataset.status = task.status;
        row.setAttribute("aria-current", String(task.id === taskId));
        const title = el("span", null, "queue-item-heading");
        title.append(el("span", taskName(task), "queue-task-title"), badge(task.status));
        const id = el("span", task.id.slice(0, 8), "queue-task-id mono"); id.title = task.id;
        const meta = el("span", null, "queue-row-meta");
        const created = el("time", new Date(task.created_at).toLocaleString("zh-CN", {month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false}));
        created.title = date(task.created_at); created.dateTime = task.created_at;
        meta.append(id, created, el("span", task.pipeline_id === "managed-v1" ? registeredSummary(task.step_count, task.completed_steps) : `${task.completed_steps}/${task.step_count} 步`));
        row.append(title, meta);
        if (!isOpenManaged(task)) {
          const progress = el("progress", null, "queue-progress");
          progress.max = task.step_count || 1; progress.value = task.completed_steps;
          progress.setAttribute("aria-label", `已成功或复用 ${task.completed_steps} / ${task.step_count} 步`);
          row.append(progress);
        }
        const wait = waits[task.wait_reason || task.phase] || task.wait_reason || (task.status === "FAILED" ? task.error_code : task.status === "CANCELING" ? "等待退出与清理" : task.cached_steps ? `${task.cached_steps} 步缓存复用` : "");
        const hint = [wait, registrationLabel(task)].filter(Boolean).join(" · ");
        if (hint) row.append(el("span", hint, `queue-wait${task.status === "FAILED" ? " error" : ""}`));
        return row;
      });
      $("queue-list").replaceChildren(...(rows.length ? rows : [el("p", queue.q || queue.status ? "没有符合条件的任务，请调整搜索或筛选。" : "暂无任务", "empty")]));
      if (focused && document.activeElement === document.body) $("queue-list").querySelector(`[data-task="${CSS.escape(focused)}"]`)?.focus({preventScroll: true});
      queueSignature = signature;
    }
    const note = [data.waiting.gpu && `GPU 等待 ${data.waiting.gpu}`, data.waiting.cpu && `CPU 等待 ${data.waiting.cpu}`, data.waiting.retry && `重试等待 ${data.waiting.retry}`].filter(Boolean).join(" · ");
    if ($("queue-message").textContent !== note) $("queue-message").textContent = note;
  }

  async function refreshMonitor() {
    if (monitorBusy) return;
    clearTimeout(monitorTimer); monitorBusy = true;
    $("monitor-refresh").disabled = true;
    const revision = monitorRevision;
    monitorController = new AbortController();
    const currentController = monitorController, deadline = setTimeout(() => currentController.abort(), 8000);
    let relocate = false;
    try {
      const query = new URLSearchParams({limit: String(queue.limit), offset: String(queue.offset)});
      if (queue.status) query.set("status", queue.status);
      if (queue.q) query.set("q", queue.q);
      const [data, resources] = await Promise.all([request(`/api/v1/monitor?${query}`, currentController.signal), request("/api/v1/resources", currentController.signal)]);
      if (revision !== monitorRevision) return;
      if (queue.offset > 0 && queue.offset >= data.filtered_total) {
        queue.offset = Math.max(0, Math.floor((data.filtered_total - 1) / queue.limit) * queue.limit);
        relocate = true;
        return;
      }
      monitorData = data; renderQueue(data);
      const signature = JSON.stringify(resources);
      if (signature !== resourceSignature) { renderResource(resources); resourceSignature = signature; }
      monitorState = "online"; monitorObserved = date(data.observed_at); renderConnection();
      if (!taskId && data.items.length && !queue.q && !queue.status && queue.offset === 0) chooseTask(data.items[0].id, {replace: true});
    } catch (error) {
      if (revision !== monitorRevision) return;
      monitorState = "offline"; renderConnection();
      $("queue-message").textContent = monitorData ? "数据可能已过期，正在重连。" : "无法读取队列，正在重试。";
    } finally {
      clearTimeout(deadline);
      if (revision === monitorRevision) {
        monitorBusy = false; $("monitor-refresh").disabled = false;
        monitorTimer = setTimeout(refreshMonitor, relocate ? 0 : 3000);
      }
    }
  }

  function restartMonitor() {
    monitorRevision += 1; monitorController?.abort(); clearTimeout(monitorTimer); monitorBusy = false;
    refreshMonitor();
  }

  function changeQueue() {
    queue.status = $("queue-status").value; queue.q = $("queue-search").value.trim(); queue.offset = 0;
    $("queue-message").textContent = "正在查询…"; restartMonitor();
  }
  function readPending(id, kind = "resume") {
    const token = `${kind}:${id}`;
    if (pending.has(token)) return pending.get(token);
    try {
      const value = JSON.parse(sessionStorage.getItem(`pipeline:${token}`));
      if (value && typeof value.key === "string" && (kind !== "resume" || Number.isInteger(value.round))) pending.set(token, value);
    } catch { /* Restricted storage still permits in-page, same-key retries. */ }
    return pending.get(token);
  }
  function savePending(id, value, kind = "resume") {
    const token = `${kind}:${id}`;
    if (value) pending.set(token, value); else pending.delete(token);
    try {
      if (value) sessionStorage.setItem(`pipeline:${token}`, JSON.stringify(value));
      else sessionStorage.removeItem(`pipeline:${token}`);
    } catch { /* Request identity remains in memory until this page closes. */ }
  }
  async function request(path, signal, options = {}) {
    const response = await fetch(path, {cache: "no-store", signal, ...options});
    const body = await response.json().catch(() => null);
    if (!response.ok) {
      const error = new Error(body?.error?.message || `HTTP ${response.status}`);
      error.status = response.status;
      error.code = body?.error?.code;
      throw error;
    }
    if (!body) throw new Error("服务响应不完整，结果尚未确认。");
    return body;
  }
  function renderActions(task) {
    $("resume-button").hidden = task.status !== "FAILED";
    $("resume-button").disabled = actionBusy;
    $("resume-button").textContent = readPending(task.id) ? "重试恢复请求" : "恢复失败任务";
    $("cancel-button").hidden = !["QUEUED", "RUNNING", "CANCELING"].includes(task.status);
    $("cancel-button").disabled = actionBusy || !["QUEUED", "RUNNING"].includes(task.status);
    $("cancel-button").textContent = task.status === "CANCELING" ? "正在取消…" : readPending(task.id, "cancel") ? "重试取消请求" : "取消任务";
    $("cancel-note").hidden = task.status !== "CANCELING";
  }
  function orderedSteps(task) {
    if (task.pipeline_id === "managed-v1") return [...task.steps].sort((a, b) => a.registration_order - b.registration_order);
    const order = new Map(task.config_snapshot.steps.map((spec, index) => [spec.key, index]));
    return [...task.steps].sort((a, b) => (order.get(a.step_key) ?? 99) - (order.get(b.step_key) ?? 99));
  }
  function renderSteps(task) {
    const steps = orderedSteps(task);
    if (followStep || !steps.some((step) => step.step_key === activeStep)) {
      activeStep = (steps.find((step) => step.status === "FAILED") || steps.find((step) => step.status === "RUNNING") || steps.find((step) => step.status === "READY") || steps.at(-1))?.step_key || "";
    }
    $("steps").dataset.pipeline = task.pipeline_id;
    const signature = JSON.stringify([steps, activeStep, followStep, steps.map((step) => (history[step.step_key] || []).map((attempt) => [attempt.id, attempt.progress, attempt.status]))]);
    const focusedStep = document.activeElement?.closest("[data-step]")?.dataset.step;
    if (stepSignature !== signature) {
      const rows = steps.map((step) => {
      const row = el("li", null, "step");
      row.dataset.step = step.step_key;
      row.dataset.status = step.status;
      row.dataset.selected = String(!followStep && activeStep === step.step_key);
      const button = el("button", null, "step-select"); button.type = "button";
      button.setAttribute("aria-pressed", String(!followStep && activeStep === step.step_key));
      button.setAttribute("aria-label", `${stepName(task, step.step_key)}，${statuses[step.status]}，查看步骤详情`);
      button.append(el("span", stepName(task, step.step_key), "step-title"), badge(step.status));
      row.append(button);
      const model = stepModel(task, step.step_key);
      if (model) row.append(el("p", `参考模型 · ${model}`, "step-model"));
      if (step.execution_node_id) row.append(el("p", executionLocation(step.execution_node_id, step.execution_resource_id), "step-meta"));
      const attempts = history[step.step_key] || [];
      const current = attempts.find((attempt) => attempt.id === step.current_attempt_id);
      const progress = current?.progress;
      if (step.status === "RUNNING" && Number.isFinite(progress?.completed_units) && progress.total_units > 0) {
        const bar = el("progress", null, "step-progress");
        bar.max = progress.total_units;
        bar.value = progress.completed_units;
        const frames = task.pipeline_id !== "managed-v1" && step.step_key === "generate_frames";
        const label = frames ? "已提交帧数" : "已提交单元";
        bar.setAttribute("aria-label", `${stepName(task, step.step_key)} ${label}`);
        row.append(bar, el("p", `${progress.completed_units} / ${progress.total_units}${frames ? " 帧" : " 单元"}`, "step-meta"));
      }
      return row;
      });
      $("steps").replaceChildren(...(rows.length ? rows : [el("li", task.pipeline_id === "managed-v1" ? "尚无已登记步骤。" : "暂无步骤记录。", "empty")])); stepSignature = signature;
      if (focusedStep && document.activeElement === document.body) $("steps").querySelector(`[data-step="${CSS.escape(focusedStep)}"] .step-select`)?.focus({preventScroll: true});
    }
    renderStepInspector(task);
    requestAnimationFrame(drawEdges);
  }

  function renderStepInspector(task) {
    $("step-inspector").hidden = followStep;
    if (followStep) return;
    const step = task.steps.find((item) => item.step_key === activeStep);
    if (!step) return;
    const attempts = history[step.step_key] || [];
    const current = attempts.find((item) => item.id === step.current_attempt_id) || attempts.at(-1);
    const heading = el("div", null, "inspector-heading");
    const close = el("button", "收起", "button-secondary"); close.type = "button"; close.dataset.closeInspector = "true";
    heading.append(el("h3", stepName(task, step.step_key)), close);
    const details = el("div", null, "inspector-facts");
    details.append(pair("执行节点", step.execution_node_id ? executionLocation(step.execution_node_id, step.execution_resource_id) : attempts.length ? "历史执行环境" : "尚未分配执行节点"), pair("执行次数", attempts.length), pair("输出资产", step.output_refs.length));
    const model = stepModel(task, step.step_key);
    if (model) details.append(pair("参考模型（演示）", model));
    if (current) details.append(pair("开始时间", date(current.started_at)), pair("运行时长", elapsed(current.started_at, current.finished_at)));
    if (task.pipeline_id === "managed-v1") details.append(pair("登记顺序", step.registration_order), pair("前置步骤", step.depends_on.map((key) => stepName(task, key)).join("、") || "无"));
    if (step.wait_reason) details.append(pair("等待原因", waits[step.wait_reason] || step.wait_reason));
    if (step.next_retry_at) details.append(pair("下次重试", date(step.next_retry_at)));
    if (current) details.append(pair("资源状态", cleanupNames[current.resource_cleanup_state] || current.resource_cleanup_state));
    const output = [heading];
    if (Object.hasOwn(task.pipeline_id === "managed-v1" ? managedNames : names, step.step_key)) output.push(el("p", stepDescriptions[step.step_key], "muted"));
    output.push(details);
    const technical = el("div", null, "step-technical");
    technical.append(pair("步骤标识", step.step_key, true), pair("执行代次", step.execution_generation), pair("节点标识", step.execution_node_id || current?.node_id, true));
    if (current) technical.append(pair("Worker", current.worker_id, true), pair("最近心跳", date(current.heartbeat_at)), pair("租约截止", date(current.lease_expires_at)));
    output.push(technical);
    if (current?.error_code) output.push(el("p", `${nodeLabel(current.error_code)} · ${current.error_code}`, "error"), el("p", current.error_message || "执行失败", "muted"));
    if (current?.resumed_from_checkpoint_id) output.push(pair("恢复检查点", current.resumed_from_checkpoint_id, true));
    if (step.status === "CACHED") {
      const link = el("a", "查看缓存来源任务", "cache-source-task");
      link.href = `/tasks/${encodeURIComponent(step.cache_source_task_id)}`; link.title = step.cache_source_task_id;
      output.push(pair("来源任务", step.cache_source_task_id, true), pair("来源步骤", step.reused_from_step_id, true), link);
    }
    $("step-inspector").replaceChildren(...output);
  }

  function drawEdges() {
    const canvas = $("flow-canvas"), svg = $("workflow-edges");
    if (!snapshot || !canvas.clientWidth || $("panel-flow").hidden) return;
    const list = $("steps"), box = canvas.getBoundingClientRect();
    const width = Math.max(canvas.clientWidth, list.offsetLeft + list.scrollWidth);
    const height = Math.max(canvas.clientHeight, list.offsetTop + list.offsetHeight);
    svg.setAttribute("width", width); svg.setAttribute("height", height); svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.style.width = `${width}px`; svg.style.height = `${height}px`;
    const ns = "http://www.w3.org/2000/svg", edges = [];
    for (const target of snapshot.steps) {
      const targetNode = list.querySelector(`[data-step="${CSS.escape(target.step_key)}"]`);
      if (!targetNode) continue;
      const to = targetNode.getBoundingClientRect();
      for (const key of target.depends_on) {
        const source = snapshot.steps.find((step) => step.step_key === key);
        const node = list.querySelector(`[data-step="${CSS.escape(key)}"]`);
        if (!node) continue;
        const from = node.getBoundingClientRect();
        const x1 = from.right - box.left + canvas.scrollLeft, y1 = (from.top + from.bottom) / 2 - box.top + canvas.scrollTop;
        const x2 = to.left - box.left + canvas.scrollLeft, y2 = (to.top + to.bottom) / 2 - box.top + canvas.scrollTop;
        const edge = document.createElementNS(ns, "path");
        edge.setAttribute("d", `M ${x1} ${y1} H ${(x1+x2)/2} V ${y2} H ${x2}`);
        edge.setAttribute("fill", "none"); edge.setAttribute("stroke-width", "1.5");
        edge.setAttribute("stroke", ["SUCCEEDED", "CACHED"].includes(source?.status) ? "#7eaa92" : "#ccd4d0");
        edges.push(edge);
      }
    }
    svg.replaceChildren(...edges);
  }
  function download(artifact, label) {
    if (artifact.availability !== "AVAILABLE") return el("span", "资产不可用", "error");
    const link = el("a", label, "download");
    link.href = `/api/v1/tasks/${taskId}/artifacts/${encodeURIComponent(artifact.id)}`;
    link.dataset.artifact = artifact.id;
    link.setAttribute("download", "");
    return link;
  }
  function artifactRow(artifact, task) {
    const row = el("li", null, "artifact-item");
    const step = task.steps.find((item) => item.id === artifact.step_id) || task.steps.find((item) => item.output_refs.includes(artifact.id));
    row.append(el("span", `${stepName(task, step?.step_key) || "生成资产"} · ${artifact.kind === "checkpoint" ? "恢复检查点" : "输出清单"}`, "artifact-name"));
    row.append(el("span", artifact.id, "mono muted"), download(artifact, "下载清单 JSON"));
    return row;
  }
  function gifPreview(artifact, id) {
    const figure = el("figure", null, "gif-preview"), image = el("img", null, "preview-image");
    const url = `/api/v1/tasks/${id}/artifacts/${encodeURIComponent(artifact.id)}/preview`;
    const message = el("p", "正在加载已提交的 GIF…", "empty");
    message.setAttribute("role", "status");
    image.alt = "任务生成结果的 GIF 动画预览";
    image.decoding = "async";
    image.addEventListener("load", () => { message.hidden = true; });
    image.addEventListener("error", () => {
      image.hidden = true;
      message.textContent = "GIF 暂时无法加载，可通过下方链接下载，或重新加载页面重试预览。";
      message.className = "error";
    });
    image.src = url;
    const link = el("a", "下载 GIF", "download");
    link.href = url;
    link.setAttribute("download", "generated-preview.gif");
    link.dataset.artifact = `${artifact.id}:preview`;
    figure.append(image, message, link);
    return figure;
  }
  function renderArtifacts(task, artifacts) {
    const outputs = artifacts.filter((item) => item.kind !== "checkpoint");
    const checkpoints = artifacts.filter((item) => item.kind === "checkpoint");
    $("artifact-summary").textContent = `${outputs.length} 个输出文件 · ${checkpoints.length} 个恢复检查点`;
    const rows = outputs.map((item) => artifactRow(item, task));
    if (checkpoints.length) {
      const row = el("li"), details = el("details"), list = el("ul");
      details.dataset.detail = "checkpoints";
      details.append(el("summary", `${checkpoints.length} 个已提交检查点`));
      list.append(...checkpoints.map((item) => artifactRow(item, task)));
      details.append(list); row.append(details); rows.push(row);
    }
    $("artifacts").replaceChildren(...(rows.length ? rows : [el("li", "尚无已提交资产，步骤完成后将在此汇总。", "empty")]));
    const final = artifacts.find((item) => task.output_refs.includes(item.id));
    if (final) {
      const facts = el("div", null, "result-facts"), preview = final.metadata?.preview, manifest = final.metadata?.result_manifest;
      facts.append(pair("结果清单", "JSON"), pair("输出文件", outputs.length), pair("恢复检查点", checkpoints.length));
      if (preview?.media_type) facts.append(pair("预览格式", preview.media_type === "image/gif" ? "GIF" : preview.media_type));
      if (preview?.width && preview?.height) facts.append(pair("预览尺寸", `${preview.width} × ${preview.height}`));
      if (manifest?.frame_count != null) facts.append(pair("生成帧数", `${manifest.frame_count} 帧`));
      $("result").replaceChildren(facts);
      if (final.availability === "AVAILABLE" && final.metadata?.preview) $("result").append(gifPreview(final, task.id));
      $("result").append(download(final, task.pipeline_id === "managed-v1" ? "下载最终结果" : "下载最终结果清单"));
    } else {
      const managedEmpty = task.status === "SUCCEEDED" ? "任务已完成，未指定最终输出文件。" : task.registration_closed ? "登记已结束，等待工作流执行完成与结果提交。" : "流程未结束；已提交的工作流资产见下方。";
      const empty = task.status === "FAILED" ? "任务失败，尚未提交最终结果。处理原因后可恢复任务。" : task.status === "CANCELED" ? "任务已取消，未提交最终结果。已有工作流资产仍保留。" : task.status === "CANCELING" ? "正在取消，等待计算进程退出与资源清理。" : task.pipeline_id === "managed-v1" ? managedEmpty : "编码与结果归档完成后，将在此展示生成结果与下载入口。";
      $("result").replaceChildren(el("p", empty, "empty"));
    }
  }
  function renderAttempts(task) {
    const rows = [];
    const cachedWithoutAttempts = orderedSteps(task).filter((step) => step.status === "CACHED" && !(history[step.step_key] || []).length);
    $("attempts-cache-note").hidden = !cachedWithoutAttempts.length;
    $("attempts-cache-note").textContent = cachedWithoutAttempts.length ? `${cachedWithoutAttempts.length} 个步骤复用已有资产，未新增执行记录` : "";
    for (const step of orderedSteps(task)) {
      for (const attempt of history[step.step_key] || []) {
        const row = el("tr");
        row.dataset.attempt = attempt.id;
        const identity = el("td");
        identity.append(el("div", `第 ${attempt.attempt_number} 次执行`), el("div", `代次 ${attempt.fencing_token}`, "muted"), el("div", attempt.id, "mono muted attempt-id"));
        const state = el("td"); state.append(badge(attempt.status));
        if (attempt.error_code) state.append(el("div", attempt.error_code, "error"));
        const timing = el("td");
        timing.append(el("div", `开始 ${date(attempt.started_at)}`), el("div", `结束 ${attempt.finished_at ? date(attempt.finished_at) : "未结束"}`), el("div", `心跳 ${date(attempt.heartbeat_at)}`, "muted"), el("div", `租约 ${date(attempt.lease_expires_at)}`, "muted"));
        const recovery = el("td");
        recovery.append(el("div", `恢复轮次 ${attempt.recovery_round}`), el("div", cleanupNames[attempt.resource_cleanup_state] || attempt.resource_cleanup_state || "—", "muted"));
        if (attempt.resumed_from_checkpoint_id) recovery.append(el("div", `检查点 ${attempt.resumed_from_checkpoint_id}`, "mono muted attempt-id"));
        const location = el("td");
        location.title = `${attempt.node_id || "legacy"} / ${attempt.resource_device_id || "—"}`;
        location.append(el("div", executionLocation(attempt.node_id, attempt.resource_device_id)));
        if (attempt.resource_slot_id != null) location.append(el("div", `槽位 ${attempt.resource_slot_id}`, "mono muted"));
        location.append(el("div", `Worker ${attempt.worker_id}`, "mono muted"));
        row.append(el("td", stepName(task, step.step_key)), identity, state, location, timing, recovery);
        rows.push(row);
      }
    }
    if (!rows.length) {
      const cell = el("td", cachedWithoutAttempts.length ? "已复用现有资产，本次未发起新的计算执行。" : "步骤尚未被 Worker 领取，暂无执行记录。", "empty"); cell.colSpan = 6;
      const row = el("tr"); row.append(cell); rows.push(row);
    }
    $("attempts").replaceChildren(...rows);
  }
  function renderEvents(task) {
    const rows = [...events.values()].sort((a, b) => b.seq - a.seq).slice(0, 100).map((event) => {
      const row = el("li", null, "event-item"); row.dataset.seq = event.seq;
      const head = el("div", null, "event-heading");
      const step = task.steps.find((item) => item.id === event.step_id);
      const title = Object.hasOwn(eventNames, event.type) ? eventNames[event.type] : event.type;
      head.append(el("strong", `${title}${step ? ` · ${stepName(task, step.step_key)}` : ""}`), el("time", date(event.created_at), "muted"));
      row.append(head, el("p", `#${event.seq} · ${event.type}`, "mono muted event-code"));
      const details = el("details");
      details.dataset.detail = `event-${event.seq}`;
      details.append(el("summary", "执行明细"), nodeEventFields({...event.payload, ...(event.attempt_id ? {attempt_id: event.attempt_id} : {})}));
      row.append(details); return row;
    });
    $("events").replaceChildren(...(rows.length ? rows : [el("li", "暂无任务事件。", "empty")]));
  }
  function cpuBudget(cpu) {
    return cpu ? `${cpu.in_use ?? "—"}/${cpu.capacity ?? "—"}` : "—";
  }
  function executionLocation(nodeId, resourceId) {
    return nodeId && nodeId !== "legacy" ? `${nodeName(nodeId)} / ${resourceId === "cpu" ? "CPU" : resourceId || "—"}` : "历史执行环境";
  }
  function resourceSlot(slot, nodeId = "") {
    const item = el("div", null, "resource-state");
    item.dataset.node = nodeId || "legacy"; item.dataset.device = nodeId ? slot.device_id : String(slot.id);
    item.append(pair("GPU 执行槽位", nodeId ? slot.device_id : `槽位 ${slot.id}`, true), badge(slot.state));
    if (slot.enabled === false) item.append(el("span", "停止分配", "badge"));
    if (slot.owner_attempt_id) {
      const owner = el("div", null, "slot-owner");
      if (slot.task_id) {
        owner.append(executionLink(slot.task_id, `任务 ${slot.task_id.slice(0, 8)}${slot.step_key ? ` · ${slot.step_key}` : ""}`, slot.step_key));
        owner.append(executionLink(slot.task_id, `执行记录 ${slot.owner_attempt_id.slice(0, 8)}`, slot.step_key, slot.owner_attempt_id));
      } else owner.append(pair("占用执行记录", slot.owner_attempt_id, true));
      if (slot.owner_generation != null) owner.append(el("span", `代次 ${slot.owner_generation}`, "mono muted"));
      item.append(owner);
    }
    if (!slot.owner_attempt_id && slot.reported_owner_attempt_id) item.append(pair("节点报告的未知占用", slot.reported_owner_attempt_id, true));
    if (slot.updated_at) item.append(pair("最近更新", date(slot.updated_at)));
    if (slot.state === "QUARANTINED") item.append(el("p", "隔离中，等待原执行退出确认。", "error"));
    return item;
  }
  const nodeLabels = {
    synthetic: "合成测试数据", uploaded: "用户上传素材",
    "durable-start-id": "幂等启动", "bound-auth": "执行请求鉴权",
    "renewable-lease": "租约续期", "confirmed-exit": "进程退出确认",
    "ordered-events": "有序事件记录", "shared-artifacts-v1": "共享资产访问",
    "committed-checkpoint-v1": "检查点恢复", "node-binding-v1": "节点与任务绑定",
    "shared-storage-inspection-v1": "共享存储可读性检查",
    NODE_CONFIGURED: "节点配置已更新", NODE_ONLINE: "节点可达",
    NODE_UNREACHABLE: "节点连接异常", NODE_OFFLINE: "节点离线",
    NODE_ACCEPTING_CHANGED: "接单状态已变更",
    STORAGE_UNAVAILABLE: "共享存储不可读", ARTIFACT_UNAVAILABLE: "产物不可读取",
    CAPABILITY_MISMATCH: "节点能力或身份不匹配", TLS_ERROR: "安全连接校验失败",
    UNREACHABLE: "无法连接节点", EXECUTOR_UNAVAILABLE: "执行器暂不可达",
    accepting: "接单状态", cpu_capacity: "CPU 并发配额", devices: "执行设备",
    error: "异常原因", code: "错误代码", message: "说明", reason: "原因", status: "节点状态",
    attempt_id: "执行记录 ID", artifact_id: "资产 ID", artifact_ids: "资产引用", output_refs: "输出资产",
    worker_id: "Worker", generation: "执行代次", attempt_number: "执行次数", node_id: "节点标识",
    resource_id: "执行设备", slot_id: "槽位编号", source: "执行来源", checkpoint_artifact_id: "恢复检查点",
    completed_batches: "已完成批次", retryable: "允许重试", will_retry: "将自动重试", recovery_round: "恢复轮次",
    pipeline_id: "工作流标识", step_key: "步骤标识", registration_order: "登记顺序", after_step: "前置步骤",
    last_step: "末尾步骤", registered_steps: "已登记步骤数", command_id: "指令 ID", executor_id: "执行器标识",
    job_id: "作业 ID", operation: "操作", source_task_id: "来源任务", source_step_id: "来源步骤", reconciled: "已完成核对",
    LEASE_LOST: "执行租约失效", ATTEMPT_TIMEOUT: "执行超时", TASK_DEADLINE: "任务截止时间已到",
    TRANSIENT_ERROR: "临时执行故障", EXECUTOR_EXIT: "执行进程异常退出", INVALID_INPUT: "输入素材无效",
    INVALID_CONFIG: "执行配置不兼容", OOM: "可用内存不足", UPSTREAM_FAILED: "上游步骤失败",
  };
  function nodeLabel(value) {
    return Object.hasOwn(nodeLabels, value) ? nodeLabels[value] : Object.hasOwn(names, value) ? names[value] : value;
  }
  const capabilityNotes = {
    "committed-checkpoint-v1": "从已提交的步骤或批次成果恢复，不表示任意模型内部断点续推。",
    "durable-start-id": "对同一启动指令去重，不保证计算只发生一次。",
    "renewable-lease": "续租用于维持执行所有权，不是任务运行时限。",
  };
  function capabilityGroup(title, values, translate = true, heading = "h4") {
    const group = el("section", null, "capability-group"), list = el("ul", null, "capability-list");
    group.append(el(heading, title));
    for (const value of values || []) {
      const label = translate ? nodeLabel(value) : value, item = el("li");
      item.append(el("span", label));
      if (label !== value) item.title = [value, Object.hasOwn(capabilityNotes, value) ? capabilityNotes[value] : ""].filter(Boolean).join(" · ");
      list.append(item);
    }
    group.append(list.children.length ? list : el("p", "未报告", "muted"));
    return group;
  }
  function nodeCapabilities(node) {
    const view = el("div", null, "node-capabilities"), data = node.capabilities;
    if (!data || !Object.keys(data).length) {
      view.append(el("p", "尚未获取节点能力。", "empty")); return view;
    }
    if (node.status !== "ONLINE") view.append(el("p", "以下为最近一次报告，当前节点不可达或观察已过期。", "resource-alert"));
    const access = data.references_readable;
    const storage = el("p", access === true ? "共享存储可读性检查：通过" : access === false ? "共享存储可读性检查：未通过" : "共享存储可读性检查：未报告", "capability-storage");
    storage.dataset.readable = access === true ? "true" : access === false ? "false" : "unknown";
    view.append(storage);
    if (data.inspection_error) view.append(pair("检查异常", nodeLabel(data.inspection_error)));
    const processors = {mock: "内置步骤处理器", "mock-step": "自定义步骤处理器"};
    const abilities = el("section", null, "execution-capabilities");
    abilities.append(el("h4", "执行能力"),
      capabilityGroup("任务处理器", Object.entries(data.processors || {}).map(([name, versions]) => `${Object.hasOwn(processors, name) ? processors[name] : name} · ${versions.map((version) => version === "independent-mock-v1" ? "独立实现 v1" : `v${version}`).join(" / ")}`), false, "h5"),
      capabilityGroup("输入来源", data.input_kinds, true, "h5"), capabilityGroup("支持的工作流步骤", data.step_keys, true, "h5"));
    const models = (data.step_keys || []).filter((key) => Object.hasOwn(demoModels, key)).map((key) => demoModels[key]);
    if (models.length) abilities.append(capabilityGroup("参考模型（演示）", models, false, "h5"));
    view.append(abilities, capabilityGroup("可靠性与安全", data.capabilities));
    const technical = el("section", null, "capability-group capability-technical"), facts = el("div", null, "node-facts");
    const cpu = pair("CPU 任务并发", `${data.cpu_in_use ?? "—"} / ${data.cpu_capacity ?? "—"}`);
    cpu.append(el("small", "节点报告 · 当前占用 / 并发上限", "muted"));
    facts.append(pair("执行协议版本", data.protocol_version), pair("资产元数据版本", data.artifact_schema_versions?.join(" / ")),
      pair("执行状态库 ID", data.state_id, true), cpu,
      pair("处理器标识", Object.entries(data.processors || {}).map(([name, versions]) => `${name}@${versions.join("/")}`).join("、"), true),
      pair("动态步骤处理器", data.named_step_processors?.join("、"), true),
      pair("输入来源标识", data.input_kinds?.join("、"), true), pair("工作流步骤标识", data.step_keys?.join("、"), true),
      pair("能力标识", data.capabilities?.join("、"), true));
    technical.append(el("h4", "运行时信息"), facts);
    view.append(technical);
    return view;
  }
  function nodeEventFields(payload) {
    const fields = el("dl", null, "node-event-fields");
    for (const [key, value] of Object.entries(payload || {})) {
      const content = el("dd");
      if (value !== null && typeof value === "object" && !Array.isArray(value)) content.append(nodeEventFields(value));
      else {
        const text = value == null ? "无" : key === "accepting" ? (value ? "接单中" : "停止接单") : typeof value === "boolean" ? (value ? "是" : "否") : Array.isArray(value) ? value.map(nodeLabel).join("、") || "无" : nodeLabel(value);
        content.textContent = key === "status" ? statuses[value] || text : key === "source" && value === "mock" ? "模拟执行" : text;
        if (typeof value === "string" && text !== value) content.append(el("small", value, "mono muted"));
      }
      fields.append(el("dt", nodeLabel(key)), content);
    }
    return fields;
  }
  function nodeResource(node) {
    const item = el("section", null, "node-resource"); item.dataset.node = node.node_id;
    const heading = el("div", null, "node-resource-heading");
    const accepting = el("span", node.accepting ? "接单中" : "停止接单", "badge node-accepting");
    accepting.dataset.accepting = String(node.accepting);
    heading.append(el("h3", nodeName(node.node_id)), badge(node.status), accepting);
    if (!node.configured) heading.append(el("span", "已移出配置", "badge"));
    item.append(heading, el("p", `CPU 任务并发 ${cpuBudget(node.cpu)} · 当前占用 / 并发上限`, "resource-budget"));
    const slots = el("div", null, "node-slots");
    slots.append(...((node.slots || []).length ? node.slots.map((slot) => resourceSlot(slot, node.node_id)) : [el("p", "未配置 GPU 执行槽位。", "empty")]));
    item.append(slots);
    if (node.last_error) item.append(el("p", typeof node.last_error === "string" ? node.last_error : JSON.stringify(node.last_error), "error"));
    const info = el("details"), facts = el("div", null, "node-facts"); info.dataset.detail = `node-info:${node.node_id}`;
    facts.append(pair("节点标识", node.node_id, true), pair("执行器标识", node.executor_id, true), pair("服务地址", node.origin, true), pair("资产存储", node.storage_id, true), pair("最近在线", date(node.last_seen_at)), pair("最近检查", date(node.last_probe_at)));
    info.append(el("summary", "节点配置与执行能力"), facts);
    info.append(nodeCapabilities(node));
    item.append(info);
    {
      const details = el("details"), list = el("ol", null, "node-events"); details.dataset.detail = `node-events:${node.node_id}`;
      for (const event of (node.events || []).slice(0, 5)) {
        const row = el("li", null, "event-item"), head = el("div", null, "event-heading");
        const type = event.type || event.event_type || "节点变化", time = el("time", date(event.created_at), "muted");
        if (event.created_at) time.setAttribute("datetime", event.created_at);
        head.append(el("strong", nodeLabel(type)), time);
        row.append(head, el("small", type, "mono muted"), nodeEventFields(event.payload)); list.append(row);
      }
      details.append(el("summary", "近期节点事件"), list.children.length ? list : el("p", "暂无节点事件。", "empty")); item.append(details);
    }
    return item;
  }
  function renderResource(resource) {
    const nodes = resource.nodes || [];
    $("nodes-status").textContent = `${nodes.filter((node) => node.status === "ONLINE").length}/${nodes.length} 在线`;
    const openDetails = [...$("node-resources").querySelectorAll("details[data-detail][open]")].map((item) => item.dataset.detail);
    $("node-resources").hidden = false;
    $("node-resources").replaceChildren(...(nodes.length ? nodes.map(nodeResource) : [el("p", "暂无配置节点。", "empty")]));
    for (const detail of $("node-resources").querySelectorAll("details[data-detail]")) if (openDetails.includes(detail.dataset.detail)) detail.open = true;
  }
  function render(task, artifacts) {
    $("task-content").hidden = false;
    $("page-message").textContent = "";
    $("task-status").replaceChildren(badge(task.status));
    $("task-title").textContent = `${taskName(task)} · ${task.id.slice(0, 8)}`;
    $("task-id").textContent = task.id;
    $("task-id").title = task.pipeline_id;
    $("task-phase").textContent = [waits[task.wait_reason || task.phase] || task.wait_reason, task.error_code].filter(Boolean).join(" · ");
    $("task-steps-summary").hidden = task.pipeline_id !== "managed-v1";
    $("task-steps-summary").textContent = task.pipeline_id === "managed-v1" ? `${registeredSummary(task.steps.length, task.steps.filter((step) => ["SUCCEEDED", "CACHED"].includes(step.status)).length)} · ${registrationLabel(task)}` : "";
    $("task-created").textContent = date(task.created_at);
    $("task-finished").textContent = task.finished_at ? date(task.finished_at) : "尚未结束";
    $("task-duration").textContent = elapsed(task.created_at, task.finished_at);
    $("task-round").textContent = task.recovery_round;
    const policy = task.config_snapshot.runtime_policy;
    $("task-policy").textContent = `心跳 ${policy.heartbeat_interval_seconds} 秒 · 可续期租约 ${policy.lease_ttl_seconds} 秒 · 单次硬时限 ${policy.attempt_hard_timeout_seconds == null ? "关闭" : `${policy.attempt_hard_timeout_seconds} 秒`} · 总截止 ${task.deadline_at ? date(task.deadline_at) : "关闭"}`;
    const operation = readPending(task.id);
    if (operation && task.recovery_round > operation.round) {
      savePending(task.id, null);
      $("action-message").textContent = "恢复请求已确认，成功步骤与已提交检查点保持保留。";
    }
    if (readPending(task.id, "cancel") && terminal.has(task.status)) {
      savePending(task.id, null, "cancel");
      $("action-message").textContent = task.status === "CANCELED" ? "后端已确认任务取消，执行退出与资源清理已完成。" : `已查询到任务${statuses[task.status]}，取消请求记录已结束，以后端终态为准。`;
    }
    renderActions(task);
    // Unchanged sections keep their DOM, keyboard focus and expanded details
    // across polling. New committed data must not close a reader's event view.
    const openDetails = [...document.querySelectorAll("details[data-detail][open]")].map((item) => item.dataset.detail);
    const focusedArtifact = document.activeElement?.dataset.artifact;
    function paint(key, value, draw) {
      const signature = JSON.stringify(value);
      if (rendered[key] !== signature) { draw(); rendered[key] = signature; }
    }
    paint("brief", [task.id, task.config_snapshot, task.steps.map((step) => step.step_key)], () => renderTaskBrief(task));
    paint("steps", [task.steps, history], () => renderSteps(task));
    paint("attempts", [task.steps, history], () => renderAttempts(task));
    paint("artifacts", [task.status, task.registration_closed, task.output_refs, artifacts], () => renderArtifacts(task, artifacts));
    paint("events", [...events.values()], () => renderEvents(task));
    for (const detail of document.querySelectorAll("details[data-detail]")) if (openDetails.includes(detail.dataset.detail)) detail.open = true;
    if (focusedArtifact && document.activeElement === document.body) document.querySelector(`[data-artifact="${CSS.escape(focusedArtifact)}"]`)?.focus({preventScroll: true});
    document.title = `${statuses[task.status] || task.status} · ${task.id.slice(0, 8)} — AIGC 任务调度 · 模拟演示`;
    revealExecution();
    if (focusDetail) { focusDetail = false; requestAnimationFrame(() => $("task-content").scrollIntoView({behavior: "smooth", block: "start"})); }
  }
  async function refresh() {
    if (!taskId || busy) return;
    clearTimeout(timer);
    busy = true;
    const current = generation, id = taskId;
    controller = new AbortController();
    const requestController = controller, signal = requestController.signal;
    const deadline = setTimeout(() => requestController.abort(), 8000);
    try {
      const base = `/api/v1/tasks/${id}`;
      const task = await request(base, signal);
      const [attemptLists, artifacts, updates] = await Promise.all([
        Promise.all(task.steps.map((step) => request(`${base}/steps/${encodeURIComponent(step.step_key)}/attempts`, signal))),
        request(`${base}/artifacts`, signal), request(`${base}/events?after_seq=${cursor}&limit=200`, signal),
      ]);
      if (current !== generation) return;
      snapshot = task;
      history = Object.fromEntries(task.steps.map((step, index) => [step.step_key, attemptLists[index]]));
      for (const event of updates) { events.set(event.seq, event); cursor = Math.max(cursor, event.seq); }
      render(task, artifacts);
      connection("online", "详情已连接 · 2 秒更新");
    } catch (error) {
      if (current !== generation) return;
      if (error.status === 404) {
        snapshot = null; $("task-content").hidden = true;
        $("page-message").textContent = "未找到该任务，请核对任务 ID。";
        connection("offline", "任务不存在");
      } else {
        connection("offline", "连接暂时中断 · 自动重试中");
        $("page-message").textContent = snapshot ? "任务数据可能已过期，正在重连。" : "无法读取任务，正在重试。";
      }
    } finally {
      clearTimeout(deadline);
      if (current === generation) {
        busy = false;
        timer = setTimeout(refresh, 2000);
      }
    }
  }
  function load(id) {
    id = id.toLowerCase();
    generation += 1; clearTimeout(timer); controller?.abort(); actionController?.abort();
    taskId = id; busy = false; actionBusy = false; snapshot = null; history = {}; events = new Map(); cursor = 0; rendered = {};
    activeStep = ""; followStep = true; stepSignature = ""; selectTab("flow");
    const query = new URLSearchParams(window.location.search);
    detailTarget = id && (query.has("step") || query.has("attempt")) ? {step: query.get("step"), attempt: query.get("attempt")} : null;
    $("task-content").hidden = true;
    $("task-metadata").open = false;
    $("step-inspector").hidden = true;
    $("action-message").textContent = "";
    $("page-message").textContent = id ? "正在读取任务执行状态…" : "请选择任务";
    connection(id ? "loading" : "idle", id ? "正在连接…" : "等待选择任务");
    document.title = "AIGC 任务调度与监控平台 · 模拟演示环境";
    if (monitorData) renderQueue(monitorData);
    if (id) refresh();
  }
  $("resume-button").addEventListener("click", async () => {
    if (actionBusy || snapshot?.status !== "FAILED") return;
    const id = taskId, current = generation;
    const operation = readPending(id) || {key: crypto.randomUUID(), round: snapshot.recovery_round};
    savePending(id, operation);
    actionBusy = true; renderActions(snapshot);
    $("action-message").textContent = "正在提交恢复请求…";
    actionController = new AbortController();
    const requestController = actionController, signal = requestController.signal;
    const deadline = setTimeout(() => requestController.abort(), 8000);
    try {
      await request(`/api/v1/tasks/${id}/resume`, signal, {method: "POST", headers: {"Idempotency-Key": operation.key}});
      if (current === generation) $("action-message").textContent = "恢复请求已提交，等待查询数据库状态。";
    } catch (error) {
      if (current === generation) $("action-message").textContent = error.status && error.status < 500 ? `恢复未生效：${error.message}` : "连接中断，恢复结果尚未确认。重试会使用同一请求标识，不会重复创建恢复轮次。";
    } finally {
      clearTimeout(deadline);
      if (current === generation) { actionBusy = false; if (snapshot) renderActions(snapshot); refresh(); refreshMonitor(); }
    }
  });
  $("cancel-button").addEventListener("click", async () => {
    if (actionBusy || !["QUEUED", "RUNNING"].includes(snapshot?.status)) return;
    const id = taskId, current = generation;
    const operation = readPending(id, "cancel") || {key: crypto.randomUUID()};
    savePending(id, operation, "cancel");
    actionBusy = true; renderActions(snapshot);
    $("action-message").textContent = "正在提交取消请求…";
    actionController = new AbortController();
    const requestController = actionController, signal = requestController.signal;
    const deadline = setTimeout(() => requestController.abort(), 8000);
    try {
      await request(`/api/v1/tasks/${id}/cancel`, signal, {method: "POST", headers: {"Idempotency-Key": operation.key}});
      if (current === generation) $("action-message").textContent = "取消请求已提交，等待后端确认进程退出、资源清理与最终状态。";
    } catch (error) {
      if (current === generation) $("action-message").textContent = error.status && error.status < 500 ? `取消请求未生效：${error.message}` : "连接中断，取消结果尚未确认。重试取消会复用同一请求键，不会调用人工恢复。";
    } finally {
      clearTimeout(deadline);
      if (current === generation) { actionBusy = false; if (snapshot) renderActions(snapshot); refresh(); refreshMonitor(); }
    }
  });
  window.addEventListener("popstate", () => load(window.location.pathname.match(/^\/tasks\/([0-9a-f-]+)\/?$/i)?.[1] || ""));
  $("queue-search-form").addEventListener("submit", (event) => {
    event.preventDefault(); changeQueue();
    const id = $("queue-search").value.trim().toLowerCase();
    if (uuid.test(id)) chooseTask(id, {scroll: true});
  });
  $("queue-status").addEventListener("change", changeQueue);
  $("monitor-refresh").addEventListener("click", () => { refreshMonitor(); refresh(); });
  $("queue-prev").addEventListener("click", () => { queue.offset = Math.max(0, queue.offset - queue.limit); restartMonitor(); });
  $("queue-next").addEventListener("click", () => { queue.offset += queue.limit; restartMonitor(); });
  $("queue-list").addEventListener("click", (event) => {
    const button = event.target.closest("[data-task]");
    if (button) chooseTask(button.dataset.task, {scroll: true});
  });
  $("resource-panel").addEventListener("click", (event) => {
    const action = event.target.closest("[data-acknowledgement]");
    if (action) { changeAcknowledgement(action.dataset.acknowledgement, action.dataset.restore === "true"); return; }
    const link = event.target.closest("[data-execution-task]");
    if (link && !event.ctrlKey && !event.metaKey && !event.shiftKey && event.button === 0) {
      event.preventDefault(); $("resource-panel").open = false; chooseTask(link.dataset.executionTask, {scroll: true});
      window.history.replaceState({}, "", link.href);
      detailTarget = {step: link.dataset.executionStep, attempt: link.dataset.executionAttempt};
      revealExecution();
    }
  });
  for (const button of document.querySelectorAll("[data-detail-tab]")) {
    button.addEventListener("click", () => selectTab(button.dataset.detailTab));
    button.addEventListener("keydown", (event) => {
      const tabs = [...document.querySelectorAll("[data-detail-tab]")], index = tabs.indexOf(button);
      const next = event.key === "ArrowRight" ? (index + 1) % tabs.length : event.key === "ArrowLeft" ? (index - 1 + tabs.length) % tabs.length : event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : -1;
      if (next >= 0) { event.preventDefault(); selectTab(tabs[next].dataset.detailTab, true); }
    });
  }
  $("steps").addEventListener("click", (event) => {
    const button = event.target.closest(".step-select");
    if (button && snapshot) { activeStep = button.closest("[data-step]").dataset.step; followStep = false; renderSteps(snapshot); }
  });
  $("step-inspector").addEventListener("click", (event) => {
    if (event.target.closest("[data-close-inspector]") && snapshot) {
      const key = activeStep; followStep = true; renderSteps(snapshot);
      $("steps").querySelector(`[data-step="${CSS.escape(key)}"] .step-select`)?.focus({preventScroll: true});
    }
  });
  const resize = new ResizeObserver(() => requestAnimationFrame(drawEdges));
  resize.observe($("flow-canvas")); resize.observe($("steps"));
  load(window.location.pathname.match(/^\/tasks\/([0-9a-f-]+)\/?$/i)?.[1] || "");
  refreshMonitor();
})();
