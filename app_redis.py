"""Redis 版：Lua 原子预扣库存 + Set 幂等 + List 消息队列 + 后台批量落库。

相比数据库行锁方案，热路径只剩一次 Redis 往返：
- Lua 脚本内完成「读库存 → 判重 → 扣减 → 入队」四步，Redis 单线程执行，
  天然串行化，不存在 check-then-act 的竞态窗口。
- request_id 写入 Set 做幂等，重复请求不会重复扣库存。
- 订单不直接写库，RPUSH 进 List，后台线程 LPOP 批量 executemany，
  把随机写放大成顺序批量写，削掉数据库写锁瓶颈。
"""
import os
import sqlite3
import threading
import time

import redis as redislib
from fastapi import FastAPI
from pydantic import BaseModel

import db

DB_PATH = os.environ.get("DB_PATH", "seckill_redis.db")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:17001/0")
STOCK_KEY = "seckill:stock:1"
DEDUP_KEY = "seckill:dedup:1"
QUEUE_KEY = "seckill:orders"

app = FastAPI(title="seckill-redis")

_pool = redislib.ConnectionPool.from_url(REDIS_URL, max_connections=128, decode_responses=True)
r = redislib.Redis(connection_pool=_pool)

LUA = """
local stock = tonumber(redis.call('GET', KEYS[1]) or '-1')
if stock <= 0 then
  return 2
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return 3
end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('RPUSH', KEYS[3], ARGV[1] .. '|' .. ARGV[2])
return 0
"""
_script = r.register_script(LUA)


class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate, self.capacity = rate, capacity
        self.tokens, self.updated = float(capacity), time.monotonic()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


BUCKET = TokenBucket(rate=50000, capacity=50000)
_stop = threading.Event()


def _consumer() -> None:
    """单进程一个消费者：批量出队 -> 一次 executemany 落库。"""
    conn = db.connect(DB_PATH)
    while not _stop.is_set():
        items = r.lpop(QUEUE_KEY, 500)
        if not items:
            time.sleep(0.005)
            continue
        rows = []
        for it in items:
            rid, uid = it.split("|", 1)
            rows.append((rid, 1, int(uid)))
        conn.executemany(
            "INSERT OR IGNORE INTO orders(request_id, sku_id, user_id, created_at) "
            "VALUES (?,?,?,datetime('now'))",
            rows,
        )


_thread = threading.Thread(target=_consumer, daemon=True)
_thread.start()


class OrderReq(BaseModel):
    request_id: str
    user_id: int = 1
    sku_id: int = 1


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/seckill")
def seckill(req: OrderReq):
    if not BUCKET.allow():
        return {"code": 429, "msg": "rate limited"}
    code = _script(keys=[STOCK_KEY, DEDUP_KEY, QUEUE_KEY], args=[req.request_id, req.user_id])
    return {"code": int(code), "msg": "ok" if int(code) == 0 else "rejected"}
