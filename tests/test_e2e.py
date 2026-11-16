"""端到端测试：通过真实 HTTP 接口驱动整个工作台。

运行：python3 -m tests.test_e2e
测试自带独立数据目录与独立端口，不影响正在运行的演示服务。
"""
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 必须在导入 app.config 之前确定独立数据目录
_TEST_DATA_DIR = Path(tempfile.mkdtemp(prefix="wb-test-"))
os.environ["WB_DATA_DIR"] = str(_TEST_DATA_DIR)

from app import config, csvutil, source as source_mod  # noqa: E402
from app.query import parse_query, full_stream_sql, count_sql  # noqa: E402
import sqlite3  # noqa: E402

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✔ {name}")
    else:
        FAIL += 1
        print(f"  ✖ {name} {detail}")


class Client:
    def __init__(self, base):
        self.base = base

    def req(self, method, path, body=None):
        raw = None
        if body is not None:
            raw = json.dumps(body).encode("utf-8")
        r = urllib.request.Request(self.base + path, data=raw, method=method)
        r.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(r, timeout=30) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8")), resp
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8")), e

    def get(self, path):
        return self.req("GET", path)[:2]

    def post(self, path, body=None):
        return self.req("POST", path, body or {})[:2]

    def stream(self, export_id, connection_id, on_frame, stop_after=None):
        """消费 NDJSON 流；stop_after=n 时收到 n 个 data 帧后主动断开。"""
        r = urllib.request.Request(
            f"{self.base}/api/exports/{export_id}/connections/{connection_id}/stream"
        )
        frames = 0
        with urllib.request.urlopen(r, timeout=30) as resp:
            buf = b""
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    frame = json.loads(line.decode("utf-8"))
                    on_frame(frame)
                    if frame["type"] == "data":
                        frames += 1
                        if stop_after and frames >= stop_after:
                            return frames  # 直接退出 → 模拟客户端断线
        return frames

    def wait_state(self, export_id, states, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            _, view = self.get(f"/api/exports/{export_id}/status")
            if view["state"] in states:
                return view
            time.sleep(0.1)
        raise AssertionError(f"导出 {export_id} 未进入 {states}，当前 {view['state']}")


def baseline_csv(sql):
    """独立计算基准 CSV：直接在当前源库上跑查询，作为字节级校对参照。

    源库路径取 config.SOURCE_DB —— 测试进程启动前已把 WB_DATA_DIR 指向临时目录，
    与被测服务进程看到的是同一份重建后的样例库。
    """
    parsed = parse_query(sql)
    conn = source_mod.connect(config.SOURCE_DB, readonly=True)
    try:
        cols = [d[0] for d in conn.execute(
            full_stream_sql(parsed) + " LIMIT 0").description if d[0] != "_wb_rn"]
        total = conn.execute(count_sql(parsed)).fetchone()[0]
        cur = conn.execute(full_stream_sql(parsed))
        hasher = hashlib.sha256()
        header = csvutil.encode_header(cols)
        hasher.update(header.encode())
        parts = [header]
        seen = 0
        for row in cur:
            text = csvutil.encode_rows(cols, [tuple(row)[:-1]])
            hasher.update(text.encode())
            parts.append(text)
            seen += 1
        assert seen == total, f"基准行数不一致 {seen} != {total}"
        return "".join(parts), hasher.hexdigest(), total, cols
    finally:
        conn.close()


class Assembler:
    """与前端相同的客户端组装规则：按绝对行号拼接，重复丢弃、缺口报错。"""
    def __init__(self, start_seq=0):
        self.parts = []
        self.seq = start_seq   # 续传场景以服务端 watermark 为本地起点
        self.columns = None
        self.checksum_expected = None

    def handle(self, frame):
        t = frame["type"]
        if t == "header":
            self.columns = frame["columns"]
            self.checksum_expected = frame["checksum"]
        elif t == "data":
            s, e = frame["start_seq"], frame["end_seq"]
            if e <= self.seq:
                return "dup"  # 完全落在已组装区间：重复帧，丢弃
            if s < self.seq:
                raise AssertionError(
                    f"帧与已组装区域重叠且跨越边界 [{s},{e})，本地已到 {self.seq}")
            if s != self.seq:
                raise AssertionError(f"行号缺口：本地 {self.seq}，帧起点 {s}")
            self.parts.append(frame["csv"])
            self.seq = e
        elif t == "error":
            raise AssertionError(f"错误帧：{frame}")
        return t

    def text(self):
        header = csvutil.encode_header(self.columns) if self.columns else ""
        return header + "".join(self.parts)

    def digest(self):
        return hashlib.sha256(self.text().encode("utf-8")).hexdigest()


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def start_server(data_dir, port):
    env = dict(os.environ, WB_DATA_DIR=str(data_dir))
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server", "--port", str(port)],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1)
            return proc
        except Exception:
            if proc.poll() is not None:
                raise RuntimeError(f"服务启动失败:\n{proc.stdout.read()}")
            time.sleep(0.2)
    proc.kill()
    raise RuntimeError("服务启动超时")


SQL_DETAIL = ("SELECT id, customer_id, region, status, amount, created_at "
              "FROM orders ORDER BY status, region, id")
SQL_AGG = ("SELECT region, status, COUNT(*) AS order_count, "
           "ROUND(SUM(amount),2) AS total_amount "
           "FROM orders GROUP BY region, status ORDER BY region, status")
SQL_AMOUNT = "SELECT id, region, status, amount FROM orders ORDER BY amount DESC, id"


def main():
    data_dir = _TEST_DATA_DIR
    port = free_port()
    print(f"数据目录={data_dir} 端口={port}")
    proc = start_server(data_dir, port)
    c = Client(f"http://127.0.0.1:{port}")
    try:
        run_all(c, data_dir, port, proc)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        shutil.rmtree(data_dir, ignore_errors=True)
    print(f"\n结果：{PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


def run_all(c, data_dir, port, proc):
    # ---------- 0. 空目录初始化（服务启动时自动建库） ----------
    print("\n[0] 空目录初始化与样例数据")
    _, summary = c.get("/api/source")
    check("源库自动重建且有数据", summary["orders"] > 0, str(summary))
    st, body = c.post("/api/source/rebuild", {"rows": 3000})
    check("可随时重建样例库", st == 200 and body["orders"] == 3000, str(body))
    _, summary = c.get("/api/source")
    check("重建后行数正确", summary["orders"] == 3000, str(summary))

    # ---------- 1. 查询校验 ----------
    print("\n[1] 查询校验（拒绝写操作、缺 ORDER BY）")
    st, body = c.post("/api/exports", {"sql": "DELETE FROM orders", "mode": "result"})
    check("写操作被拒绝", st == 400, str((st, body)))
    st, body = c.post("/api/exports",
                      {"sql": "SELECT * FROM orders", "mode": "result"})
    check("缺 ORDER BY 被拒绝", st == 400, str((st, body)))
    st, body = c.post("/api/exports", {"sql": "SELECT 1; SELECT 2 ORDER BY 1",
                                       "mode": "result"})
    check("多语句被拒绝", st == 400, str((st, body)))

    # ---------- 2. result 模式完整下载 + 断线续传 ----------
    for mode in ("result", "snapshot"):
        print(f"\n[2.{mode}] {mode} 模式：完整下载、断线续传、字节级校对")
        full_export_flow(c, mode, SQL_DETAIL)

    # ---------- 3. 重复排序键 / 聚合查询 ----------
    print("\n[3] 聚合查询与重复排序键场景")
    full_export_flow(c, "snapshot", SQL_AGG, label="聚合")
    full_export_flow(c, "result", SQL_AMOUNT, label="重复金额排序")

    # ---------- 4. 确认协议：重复确认、迟到确认、多连接 ----------
    print("\n[4] 确认协议边界")
    ack_protocol(c)

    # ---------- 5. 源表更新中的导出 ----------
    print("\n[5] 后台更新源表，导出结果保持快照一致")
    mutation_isolation(c)

    # ---------- 6. 进程恢复 ----------
    print("\n[6] 进程重启后的恢复（两种模式）")
    proc = process_recovery(c, data_dir, port, proc)

    # ---------- 7. TTL 过期 ----------
    print("\n[7] TTL 过期：保留物回收与 410 解释")
    ttl_expiry(c)

    # ---------- 8. 取消 ----------
    print("\n[8] 取消导出与取消后迟到确认")
    cancel_flow(c)

    # ---------- 9. 两种模式的空间/恢复指标对比 ----------
    print("\n[9] 保留策略指标对比")
    _, listing = c.get("/api/exports")
    modes = {e["mode"]: e for e in listing["exports"] if e["state"] in
             ("ready", "completed")}
    if "result" in modes and "snapshot" in modes:
        r, s = modes["result"], modes["snapshot"]
        print(f"    result 保留 {r['retained_bytes']} 字节；"
              f"snapshot 保留 {s['retained_bytes']} 字节")
        check("两种模式都记录了保留空间",
              r["retained_bytes"] > 0 and s["retained_bytes"] > 0)

    # ---------- 10. LIMIT/OFFSET 语义 ----------
    print("\n[10] 用户 LIMIT/OFFSET 语义")
    limit_flow(c)

    # ---------- 11. 水位重置（换客户端重新全量下载） ----------
    print("\n[11] 重置服务端水位后重新全量下载")
    reset_flow(c)


def full_export_flow(c, mode, sql, label=""):
    expected_text, expected_sum, total, _ = baseline_csv(sql)
    st, view = c.post("/api/exports",
                      {"sql": sql, "mode": mode, "ttl_seconds": 1800})
    tag = f"[{mode}{('/'+label) if label else ''}]"
    check(f"{tag} 创建成功", st == 201, str(view))
    eid = view["id"]
    view = c.wait_state(eid, ("ready", "failed"))
    check("进入 ready 且无错误", view["state"] == "ready", str(view.get("error")))
    check("总行数与基准一致", view["total_rows"] == total,
          f"{view['total_rows']} != {total}")
    check("服务端校验和与基准一致", view["checksum"] == expected_sum,
          f"{view['checksum']} != {expected_sum}")

    # 连接 1：收若干帧（每帧确认）后直接退出 → 模拟断线
    _, conn = c.post(f"/api/exports/{eid}/connections")
    conn = conn["connection"]
    asm = Assembler()
    first_frames = 3 if total >= 600 else 1

    def ack_frame(frame, conn_id=conn["id"], fence=conn["fence"]):
        asm.handle(frame)
        if frame["type"] == "data":
            st, body = c.post(f"/api/exports/{eid}/ack", {
                "connection_id": conn_id, "fence": fence,
                "seq": frame["end_seq"]})
            if st != 200:
                raise AssertionError(f"ack 失败 {st} {body}")

    got = c.stream(eid, conn["id"], ack_frame, stop_after=first_frames)
    check("首次连接收帧后断线", got >= 1)
    _, view = c.get(f"/api/exports/{eid}/status")
    check("断线后水位停在已确认位置", view["watermark"] == asm.seq,
          f"wm={view['watermark']} local={asm.seq}")

    # 若首段已收完全部行，直接做整文件校对，不再模拟续传
    if asm.seq >= total:
        check("小结果集一次收完且字节一致", asm.text() == expected_text)
        check("小结果集 sha256 一致", asm.digest() == expected_sum)
        c.post(f"/api/exports/{eid}/connections/{conn['id']}/close",
               {"reason": "test"})
        final_view = c.wait_state(eid, ("completed", "ready"))
        check("全部确认后导出完成", final_view["watermark"] == total)
        return

    # 连接 2：从共享 watermark 续传，再收若干帧后断线
    _, conn2 = c.post(f"/api/exports/{eid}/connections")
    conn2 = conn2["connection"]
    check("新连接从共享 watermark 起步", conn2["acked_seq"] == asm.seq, str(conn2))
    resume = Assembler(start_seq=asm.seq)
    second_frames = 2 if total - asm.seq >= 400 else 1

    def ack_frame2(frame):
        resume.handle(frame)
        if frame["type"] == "data":
            st, body = c.post(f"/api/exports/{eid}/ack", {
                "connection_id": conn2["id"], "fence": conn2["fence"],
                "seq": frame["end_seq"]})
            if st != 200:
                raise AssertionError(f"ack 失败 {st} {body}")

    got = c.stream(eid, conn2["id"], ack_frame2, stop_after=second_frames)
    check("续传连接只收到水位之后的新帧",
          got >= 1 and resume.seq > asm.seq, f"got={got}")
    c.post(f"/api/exports/{eid}/connections/{conn2['id']}/close",
           {"reason": "test"})

    # 连接 3：收完剩余全部
    _, conn3 = c.post(f"/api/exports/{eid}/connections")
    conn3 = conn3["connection"]
    tail = Assembler(start_seq=resume.seq)

    def ack_frame3(frame):
        tail.handle(frame)
        if frame["type"] == "data":
            st, _ = c.post(f"/api/exports/{eid}/ack", {
                "connection_id": conn3["id"], "fence": conn3["fence"],
                "seq": frame["end_seq"]})
            if st != 200:
                raise AssertionError("尾段 ack 失败")

    c.stream(eid, conn3["id"], ack_frame3)
    c.post(f"/api/exports/{eid}/connections/{conn3['id']}/close",
           {"reason": "test"})

    # 三段拼接：asm 含唯一一份 header，其余两段只有 data 文本
    full_text = asm.text() + "".join(resume.parts) + "".join(tail.parts)
    check("三段覆盖全部行（无跳行无重复）",
          tail.seq == total, f"tail 终点 {tail.seq} != 总行数 {total}")
    check("拼接文件与基准字节一致", full_text == expected_text,
          f"长度 {len(full_text)} != {len(expected_text)}")
    check("拼接文件 sha256 与服务端一致",
          hashlib.sha256(full_text.encode()).hexdigest() == expected_sum)

    final_view = c.wait_state(eid, ("completed", "ready"))
    check("全部确认后导出完成",
          final_view["watermark"] == total and
          final_view["state"] in ("completed", "ready"), str(final_view))


def ack_protocol(c):
    _, view = c.post("/api/exports", {"sql": SQL_DETAIL, "mode": "result",
                                      "ttl_seconds": 1800})
    eid = view["id"]
    c.wait_state(eid, ("ready",))
    _, conn = c.post(f"/api/exports/{eid}/connections")
    conn = conn["connection"]
    # 重复确认同一位置（幂等）：第一次是正常确认，第二次必须返回 duplicate 标记
    st, b1 = c.post(f"/api/exports/{eid}/ack",
                    {"connection_id": conn["id"], "fence": conn["fence"], "seq": 0})
    st2, b2 = c.post(f"/api/exports/{eid}/ack",
                     {"connection_id": conn["id"], "fence": conn["fence"], "seq": 0})
    check("同位置重复确认幂等",
          st == 200 and st2 == 200 and b2.get("duplicate") is True,
          f"{st2} {b2}")

    # 确认未投递的行（跳行）必须被拒
    st, body = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": conn["id"], "fence": conn["fence"],
                       "seq": 999999})
    check("拒绝确认未投递行（防跳行）", st == 409, str(body))

    # 关闭连接 → fence 提升 → 迟到确认被拒
    c.post(f"/api/exports/{eid}/connections/{conn['id']}/close",
           {"reason": "test"})
    st, body = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": conn["id"], "fence": conn["fence"],
                       "seq": 0})
    check("关闭后迟到确认被 409 拒绝", st == 409, str(body))

    # 两个连接确认同一位置：watermark 不回退、不重复
    _, ca = c.post(f"/api/exports/{eid}/connections")
    ca = ca["connection"]
    _, cb = c.post(f"/api/exports/{eid}/connections")
    cb = cb["connection"]
    st_a, ba = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": ca["id"], "fence": ca["fence"], "seq": 0})
    st_b, bb = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": cb["id"], "fence": cb["fence"], "seq": 0})
    check("多连接同位置确认安全", st_a == 200 and st_b == 200 and
          ba["watermark"] == bb["watermark"] == 0)
    c.post(f"/api/exports/{eid}/connections/{ca['id']}/close", {"reason": "t"})
    c.post(f"/api/exports/{eid}/connections/{cb['id']}/close", {"reason": "t"})

    # 不存在的连接
    st, body = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": "nope", "fence": 1, "seq": 0})
    check("未知连接被拒绝", st == 404)


def mutation_isolation(c):
    # result 模式：创建后立刻更新源表，已产出结果不应变化
    _, view = c.post("/api/exports", {"sql": SQL_DETAIL, "mode": "result",
                                      "ttl_seconds": 1800})
    eid_r = view["id"]
    c.wait_state(eid_r, ("ready",))
    before = c.get(f"/api/exports/{eid_r}/status")[1]["checksum"]
    c.post("/api/source/mutate", {"kind": "mixed", "count": 500})
    c.post("/api/source/mutate", {"kind": "insert", "count": 300})
    after = c.get(f"/api/exports/{eid_r}/status")[1]["checksum"]
    check("result 模式：源表更新不影响已完成导出", before == after)

    # snapshot 模式：快照建立后源表再变，重算结果仍等于建快照时的基准
    _, view = c.post("/api/exports", {"sql": SQL_AGG, "mode": "snapshot",
                                      "ttl_seconds": 1800})
    eid_s = view["id"]
    view = c.wait_state(eid_s, ("ready",))
    snap_sum = view["checksum"]
    c.post("/api/source/mutate", {"kind": "update", "count": 800})
    _, view2 = c.get(f"/api/exports/{eid_s}/status")
    check("snapshot 模式：源表更新后校验和不变", view2["checksum"] == snap_sum)

    # 全量下载 snapshot 导出并校对
    _, conn = c.post(f"/api/exports/{eid_s}/connections")
    conn = conn["connection"]
    asm = Assembler()
    c.stream(eid_s, conn["id"], asm.handle)
    check("snapshot 重算下载的文件 sha256 与快照时刻一致",
          asm.digest() == snap_sum)
    c.post(f"/api/exports/{eid_s}/connections/{conn['id']}/close",
           {"reason": "test"})


def process_recovery(c, data_dir, port, proc):
    # 准备两个 ready 导出（各下载一部分并确认），重启进程后继续
    ids = {}
    for mode in ("result", "snapshot"):
        _, view = c.post("/api/exports", {"sql": SQL_AMOUNT, "mode": mode,
                                          "ttl_seconds": 3600})
        eid = view["id"]
        c.wait_state(eid, ("ready",))
        _, conn = c.post(f"/api/exports/{eid}/connections")
        conn = conn["connection"]
        asm = Assembler()

        def ack_flow(frame, _conn=conn, _eid=eid):
            asm.handle(frame)
            if frame["type"] == "data":
                c.post(f"/api/exports/{_eid}/ack", {
                    "connection_id": _conn["id"], "fence": _conn["fence"],
                    "seq": frame["end_seq"]})

        c.stream(eid, conn["id"], ack_flow, stop_after=3)
        ids[mode] = (eid, asm.seq)
        proc_view = c.get(f"/api/exports/{eid}/status")[1]
        check(f"{mode}: 重启前水位已持久化={asm.seq}",
              proc_view["watermark"] == asm.seq)

    print("    重启服务进程……")
    proc.terminate()
    proc.wait(timeout=10)
    proc = start_server(data_dir, port)

    for mode, (eid, old_seq) in ids.items():
        view = c.wait_state(eid, ("ready", "completed", "failed", "expired"))
        check(f"{mode}: 重启后导出仍可恢复（state={view['state']}）",
              view["state"] in ("ready", "completed"), str(view.get("error")))
        check(f"{mode}: watermark 跨进程保持", view["watermark"] == old_seq,
              f"{view['watermark']} != {old_seq}")
        check(f"{mode}: 旧连接全部关闭且 fence 提升",
              all(conn["state"] == "closed" for conn in view["connections"]))
        # 旧连接的迟到确认必须失败
        if view["connections"]:
            old = view["connections"][0]
            st, body = c.post(f"/api/exports/{eid}/ack", {
                "connection_id": old["id"], "fence": old["fence"] - 1,
                "seq": old["acked_seq"]})
            check(f"{mode}: 旧代次迟到确认被拒", st == 409, str(body))
        # 新连接续传，从重启前持久化的 watermark 开始组装
        _, conn = c.post(f"/api/exports/{eid}/connections")
        conn = conn["connection"]
        tail = Assembler(start_seq=old_seq)

        def tail_ack(frame, _conn=conn, _eid=eid):
            tail.handle(frame)
            if frame["type"] == "data":
                c.post(f"/api/exports/{_eid}/ack", {
                    "connection_id": _conn["id"], "fence": _conn["fence"],
                    "seq": frame["end_seq"]})

        c.stream(eid, conn["id"], tail_ack)
        check(f"{mode}: 重启后续传补齐到总行数",
              tail.seq == view["total_rows"],
              f"{tail.seq} vs {view['total_rows']}")
        c.post(f"/api/exports/{eid}/connections/{conn['id']}/close",
               {"reason": "test"})
    return proc


def ttl_expiry(c):
    _, view = c.post("/api/exports", {"sql": SQL_DETAIL, "mode": "result",
                                      "ttl_seconds": config.MIN_TTL_SECONDS})
    eid = view["id"]
    c.wait_state(eid, ("ready",))
    e_dir = config.EXPORTS_DIR / eid
    shards_before = list(e_dir.glob("shard-*.csv"))
    check("过期前分片存在", len(shards_before) > 0)
    deadline = time.time() + config.MIN_TTL_SECONDS + 8
    view = None
    while time.time() < deadline:
        view = c.get(f"/api/exports/{eid}/status")[1]
        if view["state"] == "expired":
            break
        time.sleep(0.5)
    check("TTL 后状态变为 expired", view and view["state"] == "expired", str(view))
    check("分片已被删除、meta.json 保留",
          not list(e_dir.glob("shard-*.csv")) and (e_dir / "meta.json").exists())
    check("保留空间归零", view["retained_bytes"] == 0)
    # 续传被 410 拒绝且带原因
    st, body = c.post(f"/api/exports/{eid}/connections")
    check("过期后打开连接返回 410", st == 410, str(body))
    check("410 给出人话原因", "保留期" in body["error"] and "重新导出" in body["error"],
          body["error"])

    # snapshot 模式同样验证
    _, view = c.post("/api/exports", {"sql": SQL_AGG, "mode": "snapshot",
                                      "ttl_seconds": config.MIN_TTL_SECONDS})
    eid2 = view["id"]
    c.wait_state(eid2, ("ready",))
    deadline = time.time() + config.MIN_TTL_SECONDS + 8
    while time.time() < deadline:
        view = c.get(f"/api/exports/{eid2}/status")[1]
        if view["state"] == "expired":
            break
        time.sleep(0.5)
    check("snapshot 过期后快照文件删除",
          view["state"] == "expired" and
          not (config.EXPORTS_DIR / eid2 / "snapshot.db").exists())
    st, body = c.post(f"/api/exports/{eid2}/connections")
    check("snapshot 过期续传同样 410", st == 410)


def cancel_flow(c):
    _, view = c.post("/api/exports", {"sql": SQL_DETAIL, "mode": "result",
                                      "ttl_seconds": 1800})
    eid = view["id"]
    c.wait_state(eid, ("ready",))
    _, conn = c.post(f"/api/exports/{eid}/connections")
    conn = conn["connection"]
    st, _ = c.post(f"/api/exports/{eid}/cancel")
    check("取消成功", st == 200)
    view = c.get(f"/api/exports/{eid}/status")[1]
    check("状态为 cancelled 且连接全部关闭",
          view["state"] == "cancelled" and
          all(x["state"] == "closed" for x in view["connections"]))
    check("取消后保留物被释放", view["retained_bytes"] == 0)
    st, body = c.post(f"/api/exports/{eid}/ack",
                      {"connection_id": conn["id"], "fence": conn["fence"],
                       "seq": 0})
    check("取消后迟到确认被拒", st == 409, str(body))
    st, _ = c.post(f"/api/exports/{eid}/connections")
    check("取消后不能再开连接", st == 409)


def limit_flow(c):
    sql = ("SELECT id, region, amount FROM orders "
           "ORDER BY id LIMIT 100 OFFSET 10")
    expected_text, expected_sum, total, _ = baseline_csv(sql)
    check("基准总行数应用 LIMIT 钳制", total == 100, str(total))
    for mode in ("result", "snapshot"):
        _, view = c.post("/api/exports", {"sql": sql, "mode": mode,
                                          "ttl_seconds": 1800})
        eid = view["id"]
        view = c.wait_state(eid, ("ready", "failed"))
        check(f"{mode}: LIMIT 导出总行数=100", view["total_rows"] == 100, str(view))
        _, conn = c.post(f"/api/exports/{eid}/connections")
        conn = conn["connection"]
        asm = Assembler()
        c.stream(eid, conn["id"], asm.handle)
        check(f"{mode}: LIMIT 文件 sha256 与基准一致",
              asm.digest() == expected_sum,
              f"{asm.digest()} != {expected_sum}")
        c.post(f"/api/exports/{eid}/connections/{conn['id']}/close",
               {"reason": "test"})


def reset_flow(c):
    sql = SQL_AMOUNT
    expected_text, expected_sum, total, _ = baseline_csv(sql)
    _, view = c.post("/api/exports", {"sql": sql, "mode": "result",
                                      "ttl_seconds": 1800})
    eid = view["id"]
    c.wait_state(eid, ("ready",))
    # 先下载一部分并确认，制造「服务端水位已推进」的状态
    _, conn = c.post(f"/api/exports/{eid}/connections")
    conn = conn["connection"]
    asm = Assembler()

    def ack(frame):
        asm.handle(frame)
        if frame["type"] == "data":
            c.post(f"/api/exports/{eid}/ack", {
                "connection_id": conn["id"], "fence": conn["fence"],
                "seq": frame["end_seq"]})

    c.stream(eid, conn["id"], ack, stop_after=3)
    view = c.get(f"/api/exports/{eid}/status")[1]
    check("重置前水位已前进", view["watermark"] == asm.seq and asm.seq > 0)

    # 重置：旧连接关闭、水位归零
    st, view = c.post(f"/api/exports/{eid}/reset", {})
    check("重置成功且水位归零", st == 200 and view["watermark"] == 0, str(view))
    check("重置后旧连接全部关闭",
          all(x["state"] == "closed" for x in view["connections"]))
    # 旧连接的迟到确认被拒
    st, _ = c.post(f"/api/exports/{eid}/ack", {
        "connection_id": conn["id"], "fence": conn["fence"], "seq": asm.seq})
    check("重置后旧连接确认被拒", st == 409)

    # 新连接从 0 全量下载并校对
    _, conn2 = c.post(f"/api/exports/{eid}/connections")
    conn2 = conn2["connection"]
    full = Assembler()

    def ack2(frame):
        full.handle(frame)
        if frame["type"] == "data":
            c.post(f"/api/exports/{eid}/ack", {
                "connection_id": conn2["id"], "fence": conn2["fence"],
                "seq": frame["end_seq"]})

    c.stream(eid, conn2["id"], ack2)
    check("重置后重新下载的行数完整", full.seq == total)
    check("重置后文件 sha256 与基准一致", full.digest() == expected_sum)
    c.post(f"/api/exports/{eid}/connections/{conn2['id']}/close",
           {"reason": "test"})


if __name__ == "__main__":
    sys.exit(main())
