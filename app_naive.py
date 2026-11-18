"""基线版本：先查后改（check-then-act），两次独立语句，无事务、无原子性保证。

这是常见的“教科书式 CRUD”写法，在并发下会产生超卖。
连接复用策略与优化版保持一致，确保对照实验只差“并发控制”一件事。
"""
import os
import sqlite3
import threading
import time

from fastapi import FastAPI
from pydantic import BaseModel

import db

DB_PATH = os.environ.get("DB_PATH", "seckill_naive.db")
app = FastAPI(title="seckill-naive")

_local = threading.local()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = db.connect(DB_PATH)
        _local.conn = conn
    return conn


class OrderReq(BaseModel):
    request_id: str
    user_id: int = 1
    sku_id: int = 1


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/seckill")
def seckill(req: OrderReq):
    conn = get_conn()
    try:
        # 1) 先读库存
        row = conn.execute(
            "SELECT remaining FROM stock WHERE sku_id=?", (req.sku_id,)
        ).fetchone()
        if row is None or row[0] <= 0:
            return {"code": 2, "msg": "sold out"}

        # 2) 业务间隙（风控/校验/参数组装），放大竞态窗口
        time.sleep(0.002)

        # 3) 再写回：两个独立语句之间没有任何锁或版本校验
        conn.execute(
            "UPDATE stock SET remaining = remaining - 1 WHERE sku_id=?", (req.sku_id,)
        )
        conn.execute(
            "INSERT INTO orders(request_id, sku_id, user_id, created_at) "
            "VALUES (?,?,?,datetime('now'))",
            (req.request_id, req.sku_id, req.user_id),
        )
        return {"code": 0, "msg": "ok"}
    except sqlite3.IntegrityError:
        return {"code": 3, "msg": "duplicate request"}
    finally:
        pass
