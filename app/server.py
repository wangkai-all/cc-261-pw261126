"""HTTP 层：标准库 ThreadingHTTPServer，无第三方依赖。

下载协议（/api/exports/<id>/connections/<cid>/stream）：
  响应为 NDJSON（每行一个 JSON 帧），帧类型 header/data/eof/error。
  data 帧带绝对行号区间 start_seq/end_seq，客户端按行号拼接：
  重复区间丢弃、区间不连续立即报错 —— 服务端确认协议之外的第二道防线。
"""
import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from pathlib import Path

from . import config, source
from .manager import ExportError, ExportManager
from .query import QueryError

_MANAGER = None


def get_manager():
    global _MANAGER
    if _MANAGER is None:
        _MANAGER = ExportManager()
    return _MANAGER


class Handler(BaseHTTPRequestHandler):
    server_version = "Workbench/1.0"

    # ---- 基础工具 ----
    def _json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message):
        self._json(status, {"error": message, "status": status})

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            raise ExportError(400, "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ExportError(400, "请求体必须是 JSON 对象")
        return data

    def log_message(self, fmt, *args):  # 精简日志
        print(f"[{self.log_date_time_string()}] {fmt % args}", flush=True)

    # ---- 路由 ----
    def do_GET(self):
        try:
            path = urlparse(self.path).path
            if path == "/api/health":
                return self._json(200, {"ok": True, "time": time.time()})
            if path == "/api/source":
                return self._json(200, source.source_summary())
            if path == "/api/sample-queries":
                return self._json(200, {"queries": source.SAMPLE_QUERIES})
            if path == "/api/exports":
                return self._json(200, {"exports": get_manager().list_exports()})
            if m := self._match(path, "/api/exports/{}/status"):
                return self._json(200, get_manager().get(m[0]).public_view())
            if m := self._match(
                path, "/api/exports/{}/connections/{}/stream"
            ):
                return self._handle_stream(m[0], m[1])
            return self._serve_static(path)
        except ExportError as exc:
            return self._error(exc.status, exc.message)
        except Exception as exc:  # 兜底：不泄漏堆栈给页面
            self.server.exception_hook(exc)
            return self._error(500, f"服务器内部错误：{exc}")

    def do_POST(self):
        try:
            path = urlparse(self.path).path
            data = self._read_json()
            if path == "/api/source/rebuild":
                rows = int(data.get("rows") or config.DEFAULT_SOURCE_ROWS)
                orders, items = source.rebuild_source(rows=rows)
                return self._json(200, {"rebuilt": True,
                                        "orders": orders, "items": items})
            if path == "/api/source/mutate":
                stats = source.mutate_source(
                    kind=data.get("kind", "mixed"),
                    count=int(data.get("count") or 100),
                )
                return self._json(200, {"mutated": stats,
                                        "summary": source.source_summary()})
            if path == "/api/exports":
                exp = get_manager().create_export(
                    sql=data.get("sql", ""),
                    mode=data.get("mode", "result"),
                    ttl_seconds=data.get("ttl_seconds"),
                )
                return self._json(201, exp.public_view())
            if m := self._match(path, "/api/exports/{}/connections"):
                conn = get_manager().open_connection(m[0])
                return self._json(201, {"connection": conn})
            if m := self._match(path, "/api/exports/{}/ack"):
                result = get_manager().get(m[0]).ack(
                    connection_id=data["connection_id"],
                    fence=int(data["fence"]),
                    seq=int(data["seq"]),
                )
                return self._json(200, result)
            if m := self._match(path, "/api/exports/{}/cancel"):
                return self._json(200, get_manager().get(m[0]).cancel())
            if m := self._match(path, "/api/exports/{}/reset"):
                return self._json(200, get_manager().get(m[0]).reset_watermark())
            if m := self._match(path, "/api/exports/{}/connections/{}/close"):
                result = get_manager().get(m[0]).close_connection(
                    m[1], data.get("reason", "client_close")
                )
                return self._json(200, result)
            return self._error(404, f"未知接口：{path}")
        except QueryError as exc:
            return self._error(400, str(exc))
        except KeyError as exc:
            return self._error(400, f"缺少必填字段：{exc}")
        except ExportError as exc:
            return self._error(exc.status, exc.message)
        except Exception as exc:
            self.server.exception_hook(exc)
            return self._error(500, f"服务器内部错误：{exc}")

    # ---- 静态页面 ----
    def _match(self, path, pattern):
        """简单模式匹配：{} 代表单段路径变量，返回变量元组或 None。"""
        p_parts = [x for x in path.split("/") if x]
        t_parts = [x for x in pattern.split("/") if x]
        if len(p_parts) != len(t_parts):
            return None
        out = []
        for p, t in zip(p_parts, t_parts):
            if t == "{}":
                out.append(p)
            elif p != t:
                return None
        return tuple(out)

    # ---- 流式下载 ----
    def _handle_stream(self, export_id, connection_id):
        manager = get_manager()
        try:
            exp = manager.get(export_id)
        except ExportError as exc:
            return self._error(exc.status, exc.message)
        with exp.lock:
            conn = exp._get_connection_locked(connection_id)
            if not conn:
                return self._error(404, "连接不存在")
            fence = conn["fence"]
            if conn["state"] != "active":
                return self._error(409, "连接已关闭或代次失效，请重新打开连接")

        closed = threading.Event()
        # 连接活跃时间由帧投递与 ack 共同刷新；这里负责投递侧心跳
        def heartbeat():
            with exp.lock:
                c = exp._get_connection_locked(connection_id)
                if c and c["fence"] == fence:
                    c["last_active"] = round(time.time(), 3)

        def is_cancelled():
            return closed.is_set()

        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Connection-Id", connection_id)
        self.send_header("X-Fence", str(fence))
        self.end_headers()

        try:
            for frame in manager.stream(
                export_id, connection_id, fence, is_cancelled, heartbeat
            ):
                line = json.dumps(frame, ensure_ascii=False) + "\n"
                self.wfile.write(line.encode("utf-8"))
                self.wfile.flush()
        except BrokenPipeError:
            closed.set()  # 客户端掉线：连接留给 idle 回收或客户端显式 close
        except ConnectionResetError:
            closed.set()
        except ExportError as exc:
            try:
                line = json.dumps(
                    {"type": "error", "status": exc.status,
                     "message": exc.message}, ensure_ascii=False
                ) + "\n"
                self.wfile.write(line.encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                closed.set()

    # ---- 静态页面 ----
    def _serve_static(self, path):
        if path in ("", "/"):
            path = "/index.html"
        target = (config.WEB_DIR / path.lstrip("/")).resolve()
        if not str(target).startswith(str(config.WEB_DIR.resolve())) or \
                not target.is_file():
            return self._error(404, f"不存在：{path}")
        suffix = target.suffix.lower()
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "text/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(suffix, "application/octet-stream")
        body = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_server(host="127.0.0.1", port=8000):
    server = ThreadingHTTPServer((host, port), Handler)
    server.exception_hook = lambda exc: print(f"[error] {exc!r}", flush=True)
    # 首次启动若源库不存在，自动重建样例数据
    if not config.SOURCE_DB.exists():
        orders, items = source.rebuild_source()
        print(f"[init] 样例库已重建：orders={orders} items={items}", flush=True)
    return server


def main():
    import argparse
    parser = argparse.ArgumentParser(description="流式导出工作台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    server = build_server(args.host, args.port)
    manager = get_manager()
    print(f"[serve] http://{args.host}:{args.port}  数据目录={config.DATA_DIR}",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()
        server.shutdown()


if __name__ == "__main__":
    main()
