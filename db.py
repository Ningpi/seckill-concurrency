"""SQLite 初始化与连接管理：WAL + 忙等超时。"""
import os
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (
    sku_id    INTEGER PRIMARY KEY,
    remaining INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    sku_id     INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_sku ON orders(sku_id);
"""


def connect(path: str, busy_timeout_ms: int = 10000) -> sqlite3.Connection:
    """isolation_level=None => 手动控制事务。WAL 允许读写并发。"""
    conn = sqlite3.connect(path, timeout=busy_timeout_ms / 1000, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=%d" % busy_timeout_ms)
    return conn


def init(path: str, stock: int) -> None:
    for p in (path, path + "-wal", path + "-shm"):
        if os.path.exists(p):
            os.remove(p)
    conn = connect(path)
    conn.executescript(SCHEMA)
    conn.execute("INSERT INTO stock(sku_id, remaining) VALUES (1, ?)", (stock,))
    conn.close()


def snapshot(path: str) -> dict:
    conn = connect(path)
    remaining = conn.execute("SELECT remaining FROM stock WHERE sku_id=1").fetchone()[0]
    orders = conn.execute("SELECT COUNT(*) FROM orders WHERE sku_id=1").fetchone()[0]
    conn.close()
    return {"remaining": remaining, "orders": orders, "sold": 0}
