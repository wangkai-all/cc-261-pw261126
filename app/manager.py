"""导出工作台的核心状态机与确认协议。

设计要点：
- 每个导出一把可重入锁 + Condition，生产者/下载连接/reaper 之间用条件变量协作。
- 游标只有「已确认绝对行号 watermark」一个量，永远不使用业务列的值
  （排序键会重复、聚合结果没有唯一键）。
- 每个下载连接有 fence 代次：取消、连接关闭、进程重启都会让旧代次失效，
  迟到 ack 携带旧 fence 时被拒绝，因此不可能跳行或重复拼接。
- 结果分片与输入快照都按 TTL 回收；过期后保留元数据，续传得到 410 与原因说明。
"""
import csv as _csv
import hashlib
import io as _io
import json
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import config, csvutil, query as query_mod, source


class ExportError(Exception):
    """4xx 级别、可直接展示给用户的错误。"""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


# 导出状态机
STATE_RUNNING = "running"        # 结果仍在产出（result：生产者跑查询中）
STATE_READY = "ready"            # 结果/快照已就绪，可随时续传
STATE_PREPARING = "preparing"    # snapshot 模式：快照与校验和准备中
STATE_COMPLETED = "completed"    # watermark 已覆盖全部行
STATE_CANCELLED = "cancelled"
STATE_EXPIRED = "expired"        # TTL 到期，保留物已删除
STATE_FAILED = "failed"


def _now():
    return time.time()


class Export:
    """单个导出任务的全部内存状态；持久字段落在 meta.json。"""

    def __init__(self, manager, export_id, record, export_dir):
        self.manager = manager
        self.id = export_id
        self.dir = export_dir
        self.lock = threading.RLock()
        self.cond = threading.Condition(self.lock)
        self.rec = record          # 可 JSON 序列化的全部持久状态
        self.producer_stop = threading.Event()

    # ---- 持久化 ----
    @property
    def meta_path(self):
        return self.dir / "meta.json"

    def persist_locked(self):
        """调用方必须持有 self.lock。先写临时文件再原子替换。"""
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self.rec, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.meta_path)

    # ---- 便捷访问器 ----
    @property
    def state(self):
        return self.rec["state"]

    @property
    def total_rows(self):
        return self.rec.get("total_rows")

    def public_view(self):
        """对外（HTTP/页面）展示的只读视图。"""
        with self.lock:
            rec = self.rec
            return {
                "id": rec["id"],
                "mode": rec["mode"],
                "sql": rec["sql"],
                "state": rec["state"],
                "columns": rec.get("columns"),
                "total_rows": rec.get("total_rows"),
                "watermark": rec["watermark"],
                "produced_rows": rec.get("produced_rows", 0),
                "checksum": rec.get("checksum"),
                "retained_bytes": rec.get("retained_bytes", 0),
                "retained_kind": "result_shards" if rec["mode"] == "result"
                else "input_snapshot",
                "created_at": rec["created_at"],
                "expires_at": rec["expires_at"],
                "ttl_seconds": rec["ttl_seconds"],
                "error": rec.get("error"),
                "connections": [
                    {
                        "id": c["id"],
                        "fence": c["fence"],
                        "state": c["state"],
                        "acked_seq": c["acked_seq"],
                        "delivered_seq": c["delivered_seq"],
                        "last_active": c["last_active"],
                    }
                    for c in rec["connections"]
                ],
                "events": rec.get("events", [])[-20:],
            }

    def log_event_locked(self, kind, detail):
        self.rec.setdefault("events", []).append(
            {"at": round(_now(), 3), "kind": kind, "detail": detail}
        )

    # ---- 连接管理 ----
    def open_connection_locked(self):
        cid = uuid.uuid4().hex[:12]
        conn = {
            "id": cid,
            "fence": 1,
            "state": "active",
            "acked_seq": self.rec["watermark"],
            "delivered_seq": self.rec["watermark"],
            "last_active": round(_now(), 3),
        }
        self.rec["connections"].append(conn)
        self.log_event_locked("connect", {"connection": cid, "fence": 1})
        self.persist_locked()
        return conn

    def _get_connection_locked(self, connection_id):
        for c in self.rec["connections"]:
            if c["id"] == connection_id:
                return c
        return None

    def close_connection(self, connection_id, reason):
        with self.lock:
            c = self._get_connection_locked(connection_id)
            if not c:
                raise ExportError(404, "连接不存在（可能已随进程重启失效）")
            if c["state"] != "closed":
                c["state"] = "closed"
                c["fence"] += 1  # 旧流上的迟到 ack 立即作废
                self.log_event_locked(
                    "close", {"connection": connection_id, "reason": reason}
                )
                self.persist_locked()
                self.cond.notify_all()
            return {"closed": True}

    def ack(self, connection_id, fence, seq):
        """确认收到并落盘到 seq（绝对行号）。所有防重复/防跳过规则集中在这里。"""
        with self.lock:
            c = self._get_connection_locked(connection_id)
            if not c:
                raise ExportError(404, "连接不存在")
            if fence != c["fence"]:
                # 取消后迟到确认、关闭后重发：明确拒绝，绝不移动 watermark
                raise ExportError(
                    409, f"确认已过期：连接代次不匹配（当前 {c['fence']}，请求 {fence}）"
                )
            if c["state"] != "active":
                raise ExportError(409, "连接已关闭，确认被拒绝")
            if seq < c["delivered_seq"] and seq < c["acked_seq"]:
                raise ExportError(400, "确认行号非法")
            if seq > c["delivered_seq"]:
                # 不能确认从未投递过的行 —— 防止客户端跳行
                raise ExportError(
                    409, f"拒绝确认未投递的行号 {seq}（已投递到 {c['delivered_seq']}）"
                )
            if seq <= c["acked_seq"]:
                # 重复确认同一位置（含 seq=0 的重发）：幂等接受，不移动水位
                return {"watermark": self.rec["watermark"], "duplicate": True}
            c["acked_seq"] = seq
            c["last_active"] = round(_now(), 3)
            before = self.rec["watermark"]
            # 多连接共享同一 watermark，只单调前进
            self.rec["watermark"] = max(before, seq)
            advanced = self.rec["watermark"] != before
            self.log_event_locked(
                "ack",
                {"connection": connection_id, "seq": seq,
                 "watermark": self.rec["watermark"]},
            )
            total = self.rec.get("total_rows")
            if total is not None and self.rec["watermark"] >= total and \
                    self.state == STATE_READY:
                self.rec["state"] = STATE_COMPLETED
                self.log_event_locked("complete", {"watermark": total})
            self.persist_locked()
            self.cond.notify_all()
            return {"watermark": self.rec["watermark"], "advanced": advanced}

    def cancel(self):
        with self.lock:
            if self.state in (STATE_CANCELLED, STATE_EXPIRED):
                return self.public_view()
            self.rec["state"] = STATE_CANCELLED
            self.producer_stop.set()
            for c in self.rec["connections"]:
                if c["state"] == "active":
                    c["state"] = "closed"
                    c["fence"] += 1
            self.log_event_locked("cancel", {"by": "user"})
            self.persist_locked()
            self.cond.notify_all()
        # 取消即释放保留物（取消的导出没有继续的价值）
        self.manager.release_artifacts(self.id)
        return self.public_view()

    def reset_watermark(self):
        """把确认水位归零，允许从头重新下载完整文件。

        典型场景：用户换浏览器/清了本地缓存，服务端水位已推进而本地无数据。
        仅在保留物完整、状态可续时允许；现有连接全部关闭并提升 fence，
        避免旧流与新下载并发拼接。
        """
        with self.lock:
            if self.state in (STATE_EXPIRED, STATE_CANCELLED, STATE_FAILED):
                raise ExportError(409, f"当前状态 {self.state} 不允许重置下载进度")
            if not self.manager._artifacts_intact(self):
                raise ExportError(410, "保留物已不存在，无法重新下载")
            for c in self.rec["connections"]:
                if c["state"] == "active":
                    c["state"] = "closed"
                    c["fence"] += 1
            self.rec["watermark"] = 0
            self.log_event_locked("reset_watermark", {"by": "user"})
            self.persist_locked()
            self.cond.notify_all()
            return self.public_view()


class ExportManager:
    """导出注册表：创建、恢复、reaper、下载流。"""

    def __init__(self):
        config.EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
        self.exports = {}
        self._registry_lock = threading.Lock()
        self._stopping = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="wb-prod")
        self._reaper = threading.Thread(
            target=self._reap_loop, name="wb-reaper", daemon=True
        )
        self._reconcile()
        self._reaper.start()

    def shutdown(self):
        self._stopping.set()
        with self._registry_lock:
            for exp in self.exports.values():
                exp.producer_stop.set()
        self._pool.shutdown(wait=False, cancel_futures=True)

    # ---- 注册表 ----
    def _register(self, export_id, record, export_dir):
        exp = Export(self, export_id, record, export_dir)
        with self._registry_lock:
            self.exports[export_id] = exp
        return exp

    def get(self, export_id):
        exp = self.exports.get(export_id)
        if not exp:
            raise ExportError(404, f"导出 {export_id} 不存在")
        return exp

    def list_exports(self):
        with self._registry_lock:
            ids = sorted(
                self.exports,
                key=lambda i: self.exports[i].rec["created_at"],
                reverse=True,
            )
            return [self.exports[i].public_view() for i in ids]

    # ---- 连接管理 ----
    def open_connection(self, export_id):
        """打开下载连接前做状态校验；过期给出可展示的原因（410）。"""
        exp = self.get(export_id)
        with exp.lock:
            if exp.state == STATE_EXPIRED:
                raise ExportError(410, self.expiry_reason(exp))
            if exp.state == STATE_CANCELLED:
                raise ExportError(409, "导出已被取消，不能再打开下载连接")
            if exp.state == STATE_FAILED:
                raise ExportError(409, exp.rec.get("error") or "导出已失败")
            return exp.open_connection_locked()

    # ---- 创建 ----
    def create_export(self, sql, mode, ttl_seconds=None):
        parsed = query_mod.parse_query(sql)  # 非法 SQL 抛 QueryError -> 400
        if mode not in ("result", "snapshot"):
            raise ExportError(400, "mode 必须是 result 或 snapshot")
        default_ttl = (config.DEFAULT_TTL_RESULT if mode == "result"
                       else config.DEFAULT_TTL_SNAPSHOT)
        ttl = int(ttl_seconds or default_ttl)
        if ttl < config.MIN_TTL_SECONDS:
            raise ExportError(
                400, f"保留期不能短于 {config.MIN_TTL_SECONDS} 秒"
            )
        export_id = uuid.uuid4().hex[:16]
        export_dir = config.EXPORTS_DIR / export_id
        export_dir.mkdir(parents=True)
        now = round(_now(), 3)
        record = {
            "id": export_id,
            "mode": mode,
            "sql": sql,
            "parsed": parsed,
            "state": STATE_PREPARING if mode == "snapshot" else STATE_RUNNING,
            "columns": None,
            "total_rows": None,
            "watermark": 0,
            "produced_rows": 0,
            "shards": [],          # [{file, rows, start_seq, end_seq}]
            "checksum": None,
            "retained_bytes": 0,
            "ttl_seconds": ttl,
            "created_at": now,
            "ready_at": None,
            "expires_at": now + ttl,
            "connections": [],
            "events": [{"at": now, "kind": "create",
                        "detail": {"mode": mode, "ttl": ttl}}],
            "error": None,
        }
        exp = self._register(export_id, record, export_dir)
        if mode == "result":
            self._pool.submit(self._run_result_producer, export_id)
        else:
            # 快照准备在工作线程执行，HTTP 请求立即返回 preparing
            self._pool.submit(self._prepare_snapshot, export_id)
        return exp

    # ================= result 模式：流式查询 -> 分片落盘 =================
    def _run_result_producer(self, export_id):
        exp = self.get(export_id)
        try:
            parsed = exp.rec["parsed"]
            conn = source.connect(readonly=True)
            conn.execute("PRAGMA query_only = ON")
            try:
                cur = conn.execute(query_mod.full_stream_sql(parsed))
                # 最后一列是内部行号 _wb_rn，不写入导出文件
                columns = [d[0] for d in cur.description if d[0] != "_wb_rn"]
                # 有效结果集总行数（用户 LIMIT/OFFSET 已在 SQL 层生效）
                total = conn.execute(query_mod.count_sql(parsed)).fetchone()[0]
                hasher = hashlib.sha256()
                hasher.update(csvutil.encode_header(columns).encode("utf-8"))
                shard_idx = 0
                buffer_rows = []
                shard_start = 0
                produced = 0

                def flush_shard():
                    nonlocal shard_idx, buffer_rows, shard_start, produced
                    if not buffer_rows:
                        return
                    fname = f"shard-{shard_idx:06d}.csv"
                    text = csvutil.encode_rows(columns, buffer_rows)
                    (exp.dir / fname).write_text(text, encoding="utf-8")
                    hasher.update(text.encode("utf-8"))
                    end_seq = shard_start + len(buffer_rows) - 1
                    with exp.lock:
                        exp.rec["shards"].append({
                            "file": fname,
                            "rows": len(buffer_rows),
                            "start_seq": shard_start,
                            "end_seq": end_seq,
                        })
                        exp.rec["columns"] = columns
                        exp.rec["total_rows"] = total
                        exp.rec["produced_rows"] = produced
                        exp.rec["retained_bytes"] = self._dir_bytes(exp.dir)
                        exp.persist_locked()
                        exp.cond.notify_all()
                    shard_start = end_seq + 1
                    shard_idx += 1
                    buffer_rows = []

                for row in cur:  # 真正的流式迭代，不一次性物化全部结果
                    if exp.producer_stop.is_set():
                        return
                    buffer_rows.append(tuple(row)[:-1])  # 剔除内部行号列
                    produced += 1
                    if len(buffer_rows) >= config.SHARD_ROWS:
                        flush_shard()
                flush_shard()

                with exp.lock:
                    if exp.state == STATE_CANCELLED:
                        return
                    exp.rec["columns"] = columns
                    exp.rec["total_rows"] = total
                    exp.rec["produced_rows"] = produced
                    exp.rec["checksum"] = hasher.hexdigest()
                    exp.rec["retained_bytes"] = self._dir_bytes(exp.dir)
                    if produced != total:
                        raise RuntimeError(
                            f"产出行数 {produced} 与统计行数 {total} 不一致"
                        )
                    exp.rec["state"] = STATE_READY
                    exp.rec["ready_at"] = round(_now(), 3)
                    # ready 起重新计算保留期（下载窗口从就绪开始）
                    exp.rec["expires_at"] = exp.rec["ready_at"] + exp.rec["ttl_seconds"]
                    exp.log_event_locked("ready", {
                        "rows": total, "checksum": exp.rec["checksum"]})
                    exp.persist_locked()
                    exp.cond.notify_all()
            finally:
                conn.close()
        except Exception as exc:  # 生产者失败：导出不可用，明确告知而不是给半截数据
            with exp.lock:
                if exp.state == STATE_CANCELLED:
                    return
                exp.rec["state"] = STATE_FAILED
                exp.rec["error"] = f"查询执行失败：{exc}"
                exp.log_event_locked("fail", {"error": str(exc)})
                exp.persist_locked()
                exp.cond.notify_all()

    def _read_result_range(self, exp, start, end):
        """读取 result 模式的 [start, end) 绝对行区间，返回 CSV 文本。

        分片内用 csv 模块重新切分，正确处理字段中含引号/换行的情况。
        """
        parts = []
        with exp.lock:
            shards = list(exp.rec["shards"])
            columns = list(exp.rec["columns"])
        pos = start
        for sh in shards:
            if sh["end_seq"] < pos:
                continue
            if sh["start_seq"] >= end:
                break
            text = (exp.dir / sh["file"]).read_text(encoding="utf-8")
            wanted = []
            for local_idx, values in enumerate(_csv.reader(_io.StringIO(text))):
                global_seq = sh["start_seq"] + local_idx
                if pos <= global_seq < end:
                    wanted.append(values)
            if wanted:
                parts.append(csvutil.encode_rows(columns, wanted))
                pos += len(wanted)
        if pos != end:
            raise ExportError(
                503, f"请求的行区间 [{start},{end}) 尚未产出（当前可读至 {pos}）"
            )
        return "".join(parts)

    # ================= snapshot 模式：固定输入快照，恢复时重算 =================
    def _prepare_snapshot(self, export_id):
        exp = self.get(export_id)
        snap_path = exp.dir / "snapshot.db"
        tmp_path = exp.dir / "snapshot.db.tmp"
        try:
            parsed = exp.rec["parsed"]
            # 在线一致性备份：复制过程中源表继续可写，快照定格在备份事务视图上。
            # backup() 是源连接的方法，按 pages 分批复制，progress 回调可观察进度。
            src = source.connect()
            dst = sqlite3.connect(tmp_path)
            try:
                def _progress(status, remaining, total):
                    # Python sqlite3 备份 progress 回调固定三参数
                    if exp.producer_stop.is_set():
                        raise RuntimeError("快照准备被取消")

                src.backup(dst, pages=200, progress=_progress)
            finally:
                dst.close()
                src.close()
            tmp_path.replace(snap_path)

            conn = self._snapshot_connection(snap_path)
            try:
                # 列名与总数在快照上确定（此后源表再怎么变都与本导出无关）
                probe = conn.execute(query_mod.effective_sql(parsed) + " LIMIT 0")
                columns = [d[0] for d in probe.description if d[0] != "_wb_rn"]
                total = conn.execute(query_mod.count_sql(parsed)).fetchone()[0]
                # 全量校验和：重算一遍完整结果，作为最终文件校对基准
                hasher = hashlib.sha256()
                hasher.update(csvutil.encode_header(columns).encode("utf-8"))
                cur = conn.execute(query_mod.full_stream_sql(parsed))
                seen = 0
                while True:
                    if exp.producer_stop.is_set():
                        return
                    batch = cur.fetchmany(config.PAGE_ROWS)
                    if not batch:
                        break
                    batch = [tuple(r)[:-1] for r in batch]  # 剔除 _wb_rn
                    hasher.update(
                        csvutil.encode_rows(columns, batch).encode("utf-8")
                    )
                    seen += len(batch)
                if seen != total:
                    raise RuntimeError(
                        f"快照校验行数 {seen} 与统计行数 {total} 不一致"
                    )
            finally:
                conn.close()

            with exp.lock:
                if exp.state == STATE_CANCELLED:
                    return
                exp.rec["columns"] = columns
                exp.rec["total_rows"] = total
                exp.rec["checksum"] = hasher.hexdigest()
                exp.rec["state"] = STATE_READY
                exp.rec["ready_at"] = round(_now(), 3)
                exp.rec["expires_at"] = exp.rec["ready_at"] + exp.rec["ttl_seconds"]
                exp.rec["retained_bytes"] = self._dir_bytes(exp.dir)
                exp.log_event_locked("snapshot_ready", {
                    "rows": total, "checksum": exp.rec["checksum"]})
                exp.persist_locked()
                exp.cond.notify_all()
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            with exp.lock:
                if exp.state == STATE_CANCELLED:
                    return
                exp.rec["state"] = STATE_FAILED
                exp.rec["error"] = f"快照准备失败：{exc}"
                exp.log_event_locked("fail", {"error": str(exc)})
                exp.persist_locked()
                exp.cond.notify_all()

    def _snapshot_connection(self, snap_path):
        conn = sqlite3.connect(
            f"file:{snap_path}?mode=ro", uri=True, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        return conn

    # ---- 下载流（两种模式共用同一帧协议与确认语义） ----
    def stream(self, export_id, connection_id, fence, is_cancelled, heartbeat):
        """生成器：逐帧 yield dict（header/data/eof/error）。

        is_cancelled: 零参回调，HTTP 层用于检测客户端是否已断开。
        heartbeat:    零参回调，每投递一帧调用，刷新连接活跃时间。
        """
        exp = self.get(export_id)
        with exp.lock:
            self._guard_stream_locked(exp, connection_id, fence)
            conn = next(c for c in exp.rec["connections"]
                        if c["id"] == connection_id)
            start_seq = exp.rec["watermark"]
            mode = exp.rec["mode"]

        # 等待就绪（result 等首批分片；snapshot 等快照准备完成）
        with exp.lock:
            while exp.state in (STATE_RUNNING, STATE_PREPARING) and \
                    exp.rec["columns"] is None:
                if is_cancelled() or exp.producer_stop.is_set():
                    return
                exp.cond.wait(timeout=1.0)
            self._guard_stream_locked(exp, connection_id, fence)
            columns = list(exp.rec["columns"])
            total = exp.rec["total_rows"]
            # 续传起点只有一个事实来源：持久化的共享 watermark
            start_seq = exp.rec["watermark"]
            header_needed = start_seq == 0

        if header_needed:
            yield {"type": "header", "columns": columns,
                   "total_rows": total, "checksum": exp.rec.get("checksum")}
            heartbeat()

        if mode == "result":
            yield from self._stream_result(exp, conn, fence, start_seq,
                                           is_cancelled, heartbeat)
        else:
            yield from self._stream_snapshot(exp, conn, fence, start_seq,
                                             is_cancelled, heartbeat)

    def _stream_result(self, exp, conn, fence, start_seq, is_cancelled, heartbeat):
        pos = start_seq
        while True:
            with exp.lock:
                self._guard_stream_locked(exp, conn["id"], fence)
                # 首批分片落盘前 total_rows 可能尚未确定，等待生产者推进
                while exp.rec["total_rows"] is None and \
                        exp.state == STATE_RUNNING:
                    if is_cancelled() or exp.producer_stop.is_set():
                        return
                    exp.cond.wait(timeout=1.0)
                self._guard_stream_locked(exp, conn["id"], fence)
                total = exp.rec["total_rows"]
                if exp.state == STATE_FAILED:
                    yield {"type": "error", "message": exp.rec["error"]}
                    return
            if pos >= total:
                yield self._eof_frame(exp, conn)
                return
            end = min(pos + config.FRAME_ROWS, total)
            # 等这一段真正产出（边计算边下载：未产出则阻塞，而不是报错）
            with exp.lock:
                while exp.rec["produced_rows"] < end and \
                        exp.state == STATE_RUNNING:
                    if is_cancelled() or exp.producer_stop.is_set():
                        return
                    exp.cond.wait(timeout=1.0)
                self._guard_stream_locked(exp, conn["id"], fence)
                if exp.state == STATE_FAILED:
                    yield {"type": "error", "message": exp.rec["error"]}
                    return
            text = self._read_result_range(exp, pos, end)
            with exp.lock:
                conn["delivered_seq"] = end
                conn["last_active"] = round(_now(), 3)
                exp.persist_locked()
            yield {"type": "data", "start_seq": pos, "end_seq": end,
                   "rows": end - pos, "csv": text}
            heartbeat()
            pos = end

    def _stream_snapshot(self, exp, conn, fence, start_seq, is_cancelled, heartbeat):
        snap_path = exp.dir / "snapshot.db"
        if not snap_path.exists():
            yield {"type": "error",
                   "message": "快照文件已不存在，无法重算续传（可能已过期被清理）"}
            return
        db = self._snapshot_connection(snap_path)
        parsed = exp.rec["parsed"]
        try:
            pos = start_seq
            total = exp.rec["total_rows"]
            while pos < total:
                with exp.lock:
                    self._guard_stream_locked(exp, conn["id"], fence)
                end = min(pos + config.FRAME_ROWS, total)
                # 分页重算：每页按 _wb_rn 顺序取，绝对行号就是续传游标
                cur = db.execute(query_mod.page_sql(parsed, end - pos, pos))
                batch = [tuple(r)[:-1] for r in cur.fetchall()]  # 去掉 _wb_rn 列
                if len(batch) != end - pos:
                    yield {"type": "error",
                           "message": f"快照重算行数不足（{pos} 起期望 {end - pos} 行）"}
                    return
                text = csvutil.encode_rows(list(exp.rec["columns"]), batch)
                with exp.lock:
                    conn["delivered_seq"] = pos + len(batch)
                    conn["last_active"] = round(_now(), 3)
                    exp.persist_locked()
                yield {"type": "data", "start_seq": pos,
                       "end_seq": pos + len(batch),
                       "rows": len(batch), "csv": text}
                heartbeat()
                pos += len(batch)
            yield self._eof_frame(exp, conn)
        finally:
            db.close()

    def _eof_frame(self, exp, conn):
        with exp.lock:
            return {"type": "eof", "watermark": exp.rec["watermark"],
                    "total_rows": exp.rec["total_rows"],
                    "checksum": exp.rec.get("checksum")}

    def _guard_stream_locked(self, exp, connection_id, fence):
        """流上的每个动作都过一遍：代次/状态不对就立即终止。"""
        c = exp._get_connection_locked(connection_id)
        if not c:
            raise ExportError(404, "连接不存在")
        if fence != c["fence"] or c["state"] != "active":
            raise ExportError(
                409, "下载连接已失效（被取消、关闭或服务进程重启），请重新打开连接"
            )
        if exp.state == STATE_CANCELLED:
            raise ExportError(409, "导出已被取消")
        if exp.state == STATE_EXPIRED:
            raise ExportError(410, self.expiry_reason(exp))
        if exp.state == STATE_FAILED:
            raise ExportError(409, exp.rec.get("error") or "导出已失败")

    @staticmethod
    def rec_watermark(exp):
        return exp.rec["watermark"]

    @staticmethod
    def expiry_reason(exp):
        mode = exp.rec["mode"]
        if mode == "result":
            return (
                "导出结果分片已过保留期并被删除，无法从断点继续。"
                "结果保留模式不会固定输入数据，源表此刻可能已变化，"
                "重新导出得到的是新结果而非原文件；如需长期可恢复，请改用快照模式。"
            )
        return (
            "输入快照已过保留期并被删除，无法基于原数据重算续传。"
            "源表在此期间持续更新，重新导出会以当前数据生成新快照，"
            "无法保证与此前下载的字节一致。"
        )

    # ---- 保留物释放 / TTL ----
    def release_artifacts(self, export_id):
        exp = self.get(export_id)
        for p in exp.dir.iterdir():
            if p.name == "meta.json" or p.name.endswith(".tmp"):
                continue
            if p.is_file():
                p.unlink()
        with exp.lock:
            exp.rec["shards"] = []
            exp.rec["retained_bytes"] = 0
            exp.persist_locked()

    def _reap_loop(self):
        while not self._stopping.wait(1.0):
            try:
                self.reap_once()
            except Exception as exc:  # reaper 不能因为单个导出异常而死掉
                print(f"[reaper] {exc}", flush=True)

    def reap_once(self):
        """扫描并回收过期导出与僵死连接。测试也可直接调用。"""
        now = _now()
        with self._registry_lock:
            items = list(self.exports.values())
        for exp in items:
            with exp.lock:
                # 回收静默超时的下载连接：fence 提升后旧流的迟到 ack 全部作废
                for c in exp.rec["connections"]:
                    if c["state"] == "active" and \
                            now - c["last_active"] > config.CONNECTION_IDLE_TIMEOUT:
                        c["state"] = "closed"
                        c["fence"] += 1
                        exp.log_event_locked(
                            "idle_close", {"connection": c["id"]}
                        )
                if exp.state in (STATE_EXPIRED, STATE_CANCELLED, STATE_FAILED):
                    continue
                if now < exp.rec["expires_at"]:
                    continue
                self.release_artifacts(exp.id)
                exp.rec["state"] = STATE_EXPIRED
                for c in exp.rec["connections"]:
                    if c["state"] == "active":
                        c["state"] = "closed"
                        c["fence"] += 1
                exp.log_event_locked("expire", {"at": now})
                exp.persist_locked()
                exp.cond.notify_all()

    # ---- 进程恢复 ----
    def _reconcile(self):
        """启动时扫描数据目录，重建内存状态。

        - meta.json 是事实来源；watermark/连接代次持久化保证不跳行不重复。
        - result 模式：ready 过的导出分片仍在 -> 可续；跑到一半崩溃 -> 标记失败
          （不悄悄重跑，避免在已变化的源表上拼出混合版本）。
        - snapshot 模式：快照文件存在 -> 可续；准备到一半 -> 标记失败。
        """
        if not config.EXPORTS_DIR.exists():
            return
        for export_dir in sorted(config.EXPORTS_DIR.iterdir()):
            if not export_dir.is_dir():
                continue
            meta = export_dir / "meta.json"
            if not meta.exists():
                # 崩溃在 meta 首次落盘之前：清理孤儿目录
                for p in export_dir.iterdir():
                    p.unlink(missing_ok=True)
                export_dir.rmdir()
                continue
            record = json.loads(meta.read_text(encoding="utf-8"))
            exp = self._register(record["id"], record, export_dir)
            now = _now()
            with exp.lock:
                # 所有连接在进程重启时关闭并提升 fence，旧流的迟到 ack 全部拒绝
                for c in exp.rec["connections"]:
                    c["state"] = "closed"
                    c["fence"] += 1
                exp.producer_stop.set()
                mode = exp.rec["mode"]
                if exp.state in (STATE_CANCELLED, STATE_FAILED, STATE_EXPIRED):
                    pass
                elif now >= exp.rec["expires_at"]:
                    self.release_artifacts(exp.id)
                    exp.rec["state"] = STATE_EXPIRED
                    exp.log_event_locked("expire", {"at": now, "on_restart": True})
                elif exp.state == STATE_READY:
                    artifact_ok = self._artifacts_intact(exp)
                    if artifact_ok:
                        exp.log_event_locked("restart_resume", {
                            "watermark": exp.rec["watermark"]})
                    else:
                        exp.rec["state"] = STATE_FAILED
                        exp.rec["error"] = "进程恢复时发现保留文件缺失或损坏"
                        exp.log_event_locked("restart_broken", {})
                elif exp.state in (STATE_RUNNING, STATE_PREPARING):
                    # 中途崩溃：没有完整结果/快照，明确失败，要求重新发起导出
                    self.release_artifacts(exp.id)
                    exp.rec["state"] = STATE_FAILED
                    exp.rec["error"] = (
                        "服务进程在结果就绪前退出，未完成的导出不可恢复，"
                        "请重新创建导出（已确认的旧连接全部作废）"
                    )
                    exp.log_event_locked("restart_aborted", {})
                if exp.state == STATE_COMPLETED:
                    # 已完成的导出重启后仍可下载/校对（若保留物还在）
                    pass
                exp.rec["retained_bytes"] = self._dir_bytes(exp.dir)
                exp.persist_locked()

    def _artifacts_intact(self, exp):
        with exp.lock:
            if exp.rec["mode"] == "result":
                if not exp.rec["shards"]:
                    return False
                return all((exp.dir / s["file"]).exists()
                           for s in exp.rec["shards"])
            return (exp.dir / "snapshot.db").exists()

    @staticmethod
    def _dir_bytes(d):
        total = 0
        for p in Path(d).iterdir():
            if p.is_file():
                total += p.stat().st_size
        return total
