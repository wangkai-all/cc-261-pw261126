/* 流式导出工作台前端：无框架、无构建步骤。
 *
 * 下载正确性策略（双保险）：
 *  1) 服务端：连接 fence 代次 + 共享 watermark，ack 必须单调且不超过已投递；
 *  2) 客户端：只按帧的绝对行号区间拼接，重复区间丢弃、缺口立即报错，
 *     最终用服务端 sha256 校对完整 CSV。
 */
"use strict";

const $ = (id) => document.getElementById(id);
const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};

const fmtBytes = (n) => {
  if (!n) return "0 B";
  const u = ["B", "KB", "MB", "GB"];
  const i = Math.min(u.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  return `${(n / 1024 ** i).toFixed(1)} ${u[i]}`;
};
const fmtTime = (t) => t ? new Date(t * 1000).toLocaleString() : "-";

// ---------- 本地组装状态（每个导出独立，刷新页面后可由服务端水位重新续传） ----------
const local = new Map(); // exportId -> { parts: [{start,end,text}], assembledSeq, blob, checksum, columns }
const work = {
  exportId: null,
  connection: null,
  fence: null,
  controller: null,
  totalRows: null,
  expectedChecksum: null,
  columns: null,
};

function getLocal(exportId) {
  if (!local.has(exportId)) {
    local.set(exportId, { parts: [], assembledSeq: 0, checksum: null, columns: null, savedBlob: null });
  }
  return local.get(exportId);
}

function logEvent(msg) {
  const el = $("event-log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
}
function logMsg(msg, isErr) {
  const el = $("message-log");
  el.textContent += `[${new Date().toLocaleTimeString()}] ${isErr ? "✖ " : "• "}${msg}\n`;
  el.scrollTop = el.scrollHeight;
  if (isErr) console.warn(msg);
}

// ---------- 源表 ----------
async function refreshSource() {
  const s = await api("/api/source");
  $("source-status").innerHTML =
    `订单 <b>${s.orders.toLocaleString()}</b> 行 · 明细 <b>${s.items.toLocaleString()}</b> 行` +
    ` · 最近写入 ${fmtTime(s.modified_at)}`;
}
async function mutate(kind, count) {
  const r = await api("/api/source/mutate", { method: "POST", body: { kind, count } });
  logMsg(`源表变更：${JSON.stringify(r.mutated)}，当前订单 ${r.summary.orders} 行`);
  refreshSource();
}

// ---------- 预置查询 ----------
async function loadSamples() {
  const { queries } = await api("/api/sample-queries");
  const sel = $("sample-select");
  sel.innerHTML = "";
  queries.forEach((q, i) => {
    const opt = document.createElement("option");
    opt.value = i;
    opt.textContent = q.name;
    sel.appendChild(opt);
  });
  const apply = () => { $("sql-input").value = queries[Number(sel.value)].sql; };
  sel.addEventListener("change", apply);
  apply();
}

// ---------- 导出列表 ----------
async function refreshExports() {
  const { exports: list } = await api("/api/exports");
  const tbody = document.querySelector("#export-table tbody");
  tbody.innerHTML = "";
  list.forEach((e) => {
    const tr = document.createElement("tr");
    const ttl = e.expires_at ? Math.max(0, Math.round(e.expires_at - Date.now() / 1000)) : 0;
    tr.innerHTML = `
      <td class="pill">${e.id}</td>
      <td><span class="tag">${e.mode}</span></td>
      <td><span class="tag ${e.state}">${e.state}</span></td>
      <td>${e.watermark} / ${e.total_rows ?? "…"}<br>
          <small>产出 ${e.produced_rows} · 连接 ${e.connections.length}</small></td>
      <td>${e.retained_kind}<br>${fmtBytes(e.retained_bytes)}</td>
      <td>${fmtTime(e.expires_at)}<br><small>剩余 ${ttl}s</small></td>
      <td class="pill">${e.checksum ? e.checksum.slice(0, 12) + "…" : "—"}</td>
      <td></td>`;
    const btn = document.createElement("button");
    btn.textContent = "进入工作台";
    btn.onclick = () => selectExport(e.id);
    tr.lastElementChild.appendChild(btn);
    tbody.appendChild(tr);
  });
  if (work.exportId) updateWorkPanel(list.find((x) => x.id === work.exportId));
}

// ---------- 工作台 ----------
function selectExport(exportId) {
  work.exportId = exportId;
  $("work-card").hidden = false;
  $("work-id").textContent = exportId;
  logEvent(`选中导出 ${exportId}`);
  refreshExports();
}

function updateWorkPanel(e) {
  if (!e) return;
  $("work-state").textContent = e.state;
  $("work-mode").textContent = e.mode;
  $("work-watermark").textContent = e.watermark;
  $("work-total").textContent = e.total_rows ?? "…";
  work.totalRows = e.total_rows;
  work.expectedChecksum = e.checksum;
  work.columns = e.columns;
  const loc = getLocal(e.id);
  // 页面刷新后本地内存丢失：用服务端持久化的列名回填，保证续传后能正确拼表头
  if (!loc.columns && e.columns) loc.columns = e.columns;
  $("work-assembled").textContent = loc.assembledSeq;
  const c = e.connections.find((x) => x.id === work.connection?.id);
  $("work-conn").textContent = c
    ? `${c.id}（fence=${c.fence}, ${c.state}, ack=${c.acked_seq}, delivered=${c.delivered_seq}）`
    : "未连接";
  const pct = e.total_rows ? Math.round((e.watermark / e.total_rows) * 100) : 0;
  $("progress-bar").style.width = `${pct}%`;
  (e.events || []).forEach((ev) => {
    const key = `${e.id}:${ev.at}:${ev.kind}:${JSON.stringify(ev.detail)}`;
    if (!seenEvents.has(key)) {
      seenEvents.add(key);
      logEvent(`${ev.kind} ${JSON.stringify(ev.detail)}`);
    }
  });
}
const seenEvents = new Set();

// 逐帧消费 NDJSON 流
async function openStream() {
  if (!work.exportId) return;
  if (work.controller) { logMsg("已有进行中的下载", true); return; }
  try {
    const { connection } = await api(
      `/api/exports/${work.exportId}/connections`, { method: "POST", body: {} }
    );
    work.connection = connection;
    work.fence = connection.fence;
    logEvent(`打开连接 ${connection.id}，fence=${connection.fence}，起始水位=${connection.acked_seq}`);

    work.controller = new AbortController();
    const res = await fetch(
      `/api/exports/${work.exportId}/connections/${connection.id}/stream`,
      { signal: work.controller.signal }
    );
    if (!res.ok || !res.body) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || `流建立失败 HTTP ${res.status}`);
    }
    pump(res.body, connection);
  } catch (err) {
    work.controller = null;
    logMsg(err.message, true);
  }
}

async function pump(body, connection) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  const loc = getLocal(work.exportId);
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split("\n");
      buf = lines.pop();
      for (const line of lines) {
        if (!line.trim()) continue;
        const frame = JSON.parse(line);
        await handleFrame(frame, connection, loc);
      }
    }
    logMsg("流正常结束");
  } catch (err) {
    if (err.name === "AbortError") logMsg("本地中止连接（模拟断线）：已确认的行不会丢失");
    else logMsg(`流中断：${err.message}`, true);
  } finally {
    work.controller = null;
    reader.releaseLock();
    refreshExports();
  }
}

async function handleFrame(frame, connection, loc) {
  if (frame.type === "header") {
    work.totalRows = frame.total_rows;
    work.expectedChecksum = frame.checksum;
    loc.columns = frame.columns;
    logEvent(`收到表头，总行数=${frame.total_rows}，服务端校验和=${frame.checksum || "待生成"}`);
    return;
  }
  if (frame.type === "eof") {
    logEvent(`服务端 EOF：watermark=${frame.watermark}/${frame.total_rows}`);
    return;
  }
  if (frame.type === "error") {
    logMsg(`服务端错误帧：${frame.message}（status=${frame.status ?? ""}）`, true);
    if (frame.status === 410) {
      logMsg("保留期已过：保留物被回收，服务端拒绝基于过期状态续传。请新建导出。", true);
    }
    return;
  }
  if (frame.type !== "data") return;

  // 客户端第二道防线：按绝对行号区间组装
  const { start_seq: start, end_seq: end, csv } = frame;
  if (start < loc.assembledSeq) {
    if (end <= loc.assembledSeq) {
      logMsg(`丢弃重复帧 [${start},${end})（本地已到 ${loc.assembledSeq}）`);
    } else {
      throw new Error(`帧与已组装区域重叠且跨越边界，拒绝拼接 [${start},${end})`);
    }
  } else if (start > loc.assembledSeq) {
    throw new Error(`出现行号缺口：本地到 ${loc.assembledSeq}，帧却从 ${start} 开始`);
  } else {
    loc.parts.push({ start, end, text: csv });
    loc.assembledSeq = end;
  }

  // 确认给服务端（携带 fence；取消/关闭后的迟到确认会被 409 拒绝）
  try {
    const r = await api(`/api/exports/${work.exportId}/ack`, {
      method: "POST",
      body: { connection_id: connection.id, fence: work.fence, seq: end },
    });
    if (r.duplicate) logMsg(`重复确认 ${end}：服务端幂等忽略`);
  } catch (err) {
    logMsg(`ack 被拒绝：${err.message}（本帧保留在本地，重连后由水位决定去留）`, true);
  }
  $("work-assembled").textContent = loc.assembledSeq;
  if (work.totalRows) {
    $("progress-bar").style.width =
      `${Math.round((loc.assembledSeq / work.totalRows) * 100)}%`;
  }
}

function disconnect() {
  if (!work.controller) { logMsg("当前没有进行中的连接"); return; }
  work.controller.abort(); // 只中止本地流，不通知服务端 → 模拟真实断线
  work.controller = null;
  logEvent("客户端模拟断线（abort，未关闭服务端连接）");
}

async function closeConnection() {
  if (!work.connection) return logMsg("没有可关闭的连接");
  try {
    await api(
      `/api/exports/${work.exportId}/connections/${work.connection.id}/close`,
      { method: "POST", body: { reason: "user_close" } }
    );
    logEvent(`已正常关闭连接 ${work.connection.id}（fence 将提升）`);
    if (work.controller) { work.controller.abort(); work.controller = null; }
    work.connection = null;
    refreshExports();
  } catch (err) { logMsg(err.message, true); }
}

async function cancelExport() {
  if (!work.exportId) return;
  if (!confirm("确认取消整个导出？保留物会被立即释放。")) return;
  await api(`/api/exports/${work.exportId}/cancel`, { method: "POST", body: {} });
  logEvent("导出已取消，保留物已释放");
  if (work.controller) { work.controller.abort(); work.controller = null; }
  refreshExports();
}

function buildCsv(loc) {
  const header = loc.columns
    ? loc.columns.map((c) => `"${String(c).replaceAll('"', '""')}"`).join(",") + "\r\n"
    : "";
  return header + loc.parts.map((p) => p.text).join("");
}

async function verifyFile() {
  const loc = getLocal(work.exportId);
  if (!loc.assembledSeq) return logMsg("本地还没有任何数据", true);
  if (work.totalRows != null && loc.assembledSeq !== work.totalRows) {
    return logMsg(`文件不完整：本地 ${loc.assembledSeq} 行，服务端共 ${work.totalRows} 行`, true);
  }
  const csv = buildCsv(loc);
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(csv));
  loc.checksum = Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0")).join("");
  loc.savedBlob = new Blob([csv], { type: "text/csv" });
  const ok = work.expectedChecksum && loc.checksum === work.expectedChecksum;
  logEvent(`本地 sha256=${loc.checksum}`);
  if (work.expectedChecksum) {
    logEvent(`服务端   sha256=${work.expectedChecksum}`);
    logMsg(ok ? "✔ 校验和完全一致，文件无跳行、无重复、无篡改" : "✖ 校验和不一致！", !ok);
  } else {
    logMsg("服务端校验和尚未生成（结果仍在计算），请稍后再校对");
  }
}

function saveFile() {
  const loc = getLocal(work.exportId);
  if (!loc.savedBlob) return logMsg("请先执行校对以生成文件", true);
  const url = URL.createObjectURL(loc.savedBlob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `export-${work.exportId}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function resetLocal() {
  if (!confirm("清空本地组装结果？服务端确认水位不变，继续下载时重复帧将被丢弃。")) return;
  local.set(work.exportId, { parts: [], assembledSeq: 0, checksum: null, columns: work.columns, savedBlob: null });
  $("work-assembled").textContent = "0";
  $("progress-bar").style.width = "0%";
  logMsg("本地组装结果已清空");
}

async function resetServerAndRedownload() {
  if (!work.exportId) return;
  if (!confirm("重置服务端确认水位到 0？现有连接全部关闭，之后可从头下载完整文件。")) return;
  if (work.controller) { work.controller.abort(); work.controller = null; }
  try {
    await api(`/api/exports/${work.exportId}/reset`, { method: "POST", body: {} });
    local.set(work.exportId, { parts: [], assembledSeq: 0, checksum: null, columns: work.columns, savedBlob: null });
    work.connection = null;
    $("work-assembled").textContent = "0";
    logEvent("服务端水位已重置为 0，本地缓存已清空，可重新全量下载");
    refreshExports();
  } catch (err) { logMsg(err.message, true); }
}

// ---------- 创建导出 ----------
async function createExport() {
  try {
    const e = await api("/api/exports", {
      method: "POST",
      body: {
        sql: $("sql-input").value,
        mode: $("mode-select").value,
        ttl_seconds: Number($("ttl-input").value),
      },
    });
    logMsg(`已创建导出 ${e.id}（${e.mode}，TTL=${e.ttl_seconds}s）`);
    selectExport(e.id);
    refreshExports();
  } catch (err) {
    $("query-hint").textContent = err.message;
    logMsg(err.message, true);
  }
}

// ---------- 初始化 ----------
function bind() {
  $("btn-refresh-source").onclick = refreshSource;
  $("btn-mutate-insert").onclick = () => mutate("insert", 100);
  $("btn-mutate-update").onclick = () => mutate("update", 100);
  $("btn-mutate-delete").onclick = () => mutate("delete", 100);
  $("btn-mutate-mixed").onclick = () => mutate("mixed", 200);
  $("btn-rebuild").onclick = async () => {
    if (!confirm("重建会清空并重新生成样例库，已有导出的源数据视图不受影响（快照独立）。继续？")) return;
    const r = await api("/api/source/rebuild", { method: "POST", body: {} });
    logMsg(`样例库重建完成：orders=${r.orders}, items=${r.items}`);
    refreshSource();
  };
  $("btn-create").onclick = createExport;
  $("btn-refresh-exports").onclick = refreshExports;
  $("btn-open").onclick = openStream;
  $("btn-disconnect").onclick = disconnect;
  $("btn-close-conn").onclick = closeConnection;
  $("btn-cancel").onclick = cancelExport;
  $("btn-verify").onclick = verifyFile;
  $("btn-save").onclick = saveFile;
  $("btn-reset-local").onclick = resetLocal;
  $("btn-reset-server").onclick = resetServerAndRedownload;

  setInterval(() => { if ($("auto-poll").checked) refreshExports(); }, 1500);
}

bind();
loadSamples();
refreshSource();
refreshExports();
