"""优化版本：原子扣减（CAS）+ 显式事务 + 忙等重试 + 连接复用
+ 令牌桶限流 + 订单异步批量落库（削峰）。

关键点：
1. `UPDATE ... WHERE remaining > 0` 把“读-判断-写”合并成一条原子语句，
   由数据库行锁保证不超卖，不再依赖应用层判断。
2. BEGIN IMMEDIATE 提前拿写锁，避免读事务升级写事务时的死锁 / BUSY。
3. SQLITE_BUSY 指数退避重试，多 worker（多进程）共用一个库仍然稳定。
4. 线程本地连接复用，省掉每请求建连与 PRAGMA 开销。
5. 进程内令牌桶限流，用 threading.Lock 保护，挡住突发流量。
6. 订单流水放入 queue，由后台线程批量 executemany 落库，
   把“每次请求一次磁盘写”降为“每 N 条一次批量写”，降低尾延迟。
   代价：响应返回时订单可能尚未落盘（秒杀场景可接受，库存已原子扣减）。
"""
import os
import queue
import sqlite3
import threading
import time

from fastapi import FastAPI
from pydantic import BaseModel

import db

DB_PATH = os.environ.get("DB_PATH", "seckill_opt.db")
app = FastAPI(title="seckill-optimized")

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = db.connect(DB_PATH)
        _local.conn = conn
    return conn


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def allow(self, cost: int = 1) -> bool:
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= cost:
                self.tokens -= cost
                return True
            return False


BUCKET = TokenBucket(rate=20000, capacity=20000)


# ---------- 订单异步批量落库 ----------
_order_q: "queue.Queue[tuple]" = queue.Queue(maxsize=100000)
_stop = threading.Event()
_pending = 0
_pending_lock = threading.Lock()


def _flush_loop() -> None:
    global _pending
    conn = db.connect(DB_PATH)
    buf = []
    while not _stop.is_set():
        try:
            buf.append(_order_q.get(timeout=0.02))
        except queue.Empty:
            pass
        while len(buf) < 500:
            try:
                buf.append(_order_q.get_nowait())
            except queue.Empty:
                break
        if buf:
            conn.executemany(
                "INSERT OR IGNORE INTO orders(request_id, sku_id, user_id, created_at) "
                "VALUES (?,?,?,datetime('now'))",
                buf,
            )
            with _pending_lock:
                _pending -= len(buf)
            buf.clear()
    conn.close()


_flush_thread = threading.Thread(target=_flush_loop, daemon=True)
_flush_thread.start()


class OrderReq(BaseModel):
    request_id: str
    user_id: int = 1
    sku_id: int = 1


@app.get("/health")
def health():
    return {"ok": True, "pending": _pending}


def _grab_stock(conn: sqlite3.Connection, req: OrderReq) -> int:
    """返回 0=成功，2=已售罄。"""
    global _pending
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE stock SET remaining = remaining - 1 "
            "WHERE sku_id = ? AND remaining > 0",
            (req.sku_id,),
        )
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            return 2
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    _order_q.put((req.request_id, req.sku_id, req.user_id))
    with _pending_lock:
        _pending += 1
    return 0


@app.post("/seckill")
def seckill(req: OrderReq):
    if not BUCKET.allow():
        return {"code": 429, "msg": "rate limited"}

    conn = get_conn()
    delay = 0.002
    for attempt in range(6):
        try:
            code = _grab_stock(conn, req)
            return {"code": code, "msg": "ok" if code == 0 else "sold out"}
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                raise
            if attempt == 5:
                return {"code": 500, "msg": "db busy"}
            time.sleep(delay)
            delay = min(delay * 2, 0.05)
    return {"code": 500, "msg": "unreachable"}
