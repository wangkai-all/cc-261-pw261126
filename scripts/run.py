#!/usr/bin/env python3
"""命令行入口：重建样例库 / 启动服务 / 运行测试。

用法：
  python3 scripts/run.py rebuild [--rows N]   从零重建可复现的样例数据
  python3 scripts/run.py serve [--port 8000]  启动流式导出工作台
  python3 scripts/run.py test                 运行端到端测试（独立临时目录）
"""
import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description="流式导出工作台")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_rebuild = sub.add_parser("rebuild", help="重建样例源库")
    p_rebuild.add_argument("--rows", type=int, default=None)
    p_rebuild.add_argument("--seed", type=int, default=20261116)

    p_serve = sub.add_parser("serve", help="启动 HTTP 服务")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("test", help="运行端到端测试")

    args = parser.parse_args()

    if args.cmd == "rebuild":
        from app import source
        orders, items = source.rebuild_source(rows=args.rows, seed=args.seed)
        print(f"样例库已重建：orders={orders} order_items={items}")
        print(f"位置：{os.environ.get('WB_DATA_DIR', ROOT / 'data')}")
    elif args.cmd == "serve":
        from app.server import build_server, get_manager
        server = build_server(args.host, args.port)
        get_manager()  # 提前初始化（触发 reaper 与恢复扫描）
        print(f"服务已启动：http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            get_manager().shutdown()
            server.shutdown()
    elif args.cmd == "test":
        import tests.test_e2e as t
        sys.exit(t.main())


if __name__ == "__main__":
    main()
