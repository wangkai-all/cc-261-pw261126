"""源数据表：本地 SQLite，全部样例数据可由固定随机种子重建。

两张表：
  orders      订单主表（region/status 是低基数列，天然产生重复排序键）
  order_items 订单明细（用于聚合查询演示）
"""
import random
import sqlite3
import time
from datetime import datetime, timedelta

from . import config

REGIONS = ["华东", "华北", "华南", "西南", "华中", "东北", "西北"]
STATUSES = ["pending", "paid", "shipped", "done", "cancelled", "refunded"]
SKUS = [f"SKU-{i:04d}" for i in range(1, 51)]


def connect(db_path=None, readonly=False):
    """打开源库连接。只读连接走 immutable 之外的 readonly URI，允许与写者共存。"""
    path = str(db_path or config.SOURCE_DB)
    if readonly:
        conn = sqlite3.connect(
            f"file:{path}?mode=ro", uri=True, check_same_thread=False
        )
    else:
        conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _create_schema(conn):
    conn.executescript(
        """
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            region TEXT NOT NULL,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            sku TEXT NOT NULL,
            qty INTEGER NOT NULL,
            unit_price REAL NOT NULL
        );
        CREATE INDEX idx_orders_status_region ON orders(status, region);
        CREATE INDEX idx_orders_amount ON orders(amount);
        CREATE INDEX idx_orders_created ON orders(created_at);
        CREATE INDEX idx_items_order ON order_items(order_id);
        """
    )


def rebuild_source(rows=None, seed=20261116):
    """从零重建样例库。固定种子保证可重复；返回 (订单数, 明细数)。"""
    rows = rows or config.DEFAULT_SOURCE_ROWS
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    # 先删旧文件（含 wal/shm），避免残留影响重建
    for suffix in ("", "-wal", "-shm"):
        p = config.SOURCE_DB.with_name(config.SOURCE_DB.name + suffix)
        p.unlink(missing_ok=True)

    rnd = random.Random(seed)
    base_time = datetime(2026, 1, 1, 0, 0, 0)
    conn = connect()
    try:
        _create_schema(conn)
        order_rows = []
        item_rows = []
        item_id = 1
        for oid in range(1, rows + 1):
            order_rows.append((
                oid,
                rnd.randint(1000, 9999),
                rnd.choice(REGIONS),
                rnd.choice(STATUSES),
                round(rnd.uniform(5.0, 5000.0), 2),
                (base_time + timedelta(minutes=rnd.randint(0, 60 * 24 * 300)))
                .strftime("%Y-%m-%d %H:%M:%S"),
            ))
            for _ in range(rnd.randint(1, 4)):
                item_rows.append((
                    item_id, oid, rnd.choice(SKUS),
                    rnd.randint(1, 20), round(rnd.uniform(1.0, 800.0), 2),
                ))
                item_id += 1
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)", order_rows
        )
        conn.executemany(
            "INSERT INTO order_items VALUES (?, ?, ?, ?, ?)", item_rows
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    return rows, len(item_rows)


def source_summary():
    conn = connect(readonly=True)
    try:
        orders = conn.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"]
        items = conn.execute("SELECT COUNT(*) c FROM order_items").fetchone()["c"]
        mtime = config.SOURCE_DB.stat().st_mtime if config.SOURCE_DB.exists() else 0
    finally:
        conn.close()
    return {"orders": orders, "items": items, "modified_at": mtime}


def mutate_source(kind="mixed", count=100, seed=None):
    """在源表上制造真实更新，用于演示「后台数据还在变」。

    insert/update/delete/mixed 四种操作；返回各类操作的实际条数。
    """
    rnd = random.Random(seed if seed is not None else time.time_ns())
    count = max(1, min(int(count), 5000))
    stats = {"insert": 0, "update": 0, "delete": 0}
    conn = connect()
    try:
        max_id = conn.execute("SELECT COALESCE(MAX(id),0) m FROM orders").fetchone()["m"]
        existing = [r["id"] for r in conn.execute("SELECT id FROM orders").fetchall()]
        next_id = max_id + 1
        base_time = datetime(2026, 1, 1)
        for _ in range(count):
            action = kind if kind in ("insert", "update", "delete") else rnd.choice(
                ["insert", "update", "delete"]
            )
            if action == "insert":
                conn.execute(
                    "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                    (next_id, rnd.randint(1000, 9999), rnd.choice(REGIONS),
                     rnd.choice(STATUSES), round(rnd.uniform(5, 5000), 2),
                     (base_time + timedelta(minutes=rnd.randint(0, 60 * 24 * 320)))
                     .strftime("%Y-%m-%d %H:%M:%S")),
                )
                n_items = rnd.randint(1, 3)
                for k in range(n_items):
                    conn.execute(
                        "INSERT INTO order_items VALUES (NULL, ?, ?, ?, ?)",
                        (next_id, rnd.choice(SKUS), rnd.randint(1, 20),
                         round(rnd.uniform(1, 800), 2)),
                    )
                next_id += 1
                stats["insert"] += 1
            elif action == "update" and existing:
                oid = rnd.choice(existing)
                if rnd.random() < 0.5:
                    conn.execute(
                        "UPDATE orders SET status = ?, amount = ? WHERE id = ?",
                        (rnd.choice(STATUSES),
                         round(rnd.uniform(5, 5000), 2), oid),
                    )
                else:
                    conn.execute(
                        "UPDATE orders SET region = ? WHERE id = ?",
                        (rnd.choice(REGIONS), oid),
                    )
                stats["update"] += 1
            elif action == "delete" and existing:
                oid = rnd.choice(existing)
                conn.execute("DELETE FROM order_items WHERE order_id = ?", (oid,))
                conn.execute("DELETE FROM orders WHERE id = ?", (oid,))
                existing.remove(oid)
                stats["delete"] += 1
        conn.commit()
    finally:
        conn.close()
    return stats


# 页面预置的演示查询（含明细、聚合、重复排序键三种场景）
SAMPLE_QUERIES = [
    {
        "name": "订单明细 · 重复排序键(status, region)",
        "sql": ("SELECT id, customer_id, region, status, amount, created_at "
                "FROM orders ORDER BY status, region, id"),
    },
    {
        "name": "聚合查询 · 地区×状态分组",
        "sql": ("SELECT region, status, COUNT(*) AS order_count, "
                "ROUND(SUM(amount), 2) AS total_amount "
                "FROM orders GROUP BY region, status "
                "ORDER BY region, status"),
    },
    {
        "name": "金额排序 · 大量重复金额值",
        "sql": "SELECT id, region, status, amount FROM orders ORDER BY amount DESC, id",
    },
]
