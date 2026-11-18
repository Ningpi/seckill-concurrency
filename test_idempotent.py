"""幂等性验证：并发发送同一个 request_id，必须只扣一次库存、只生成一条订单。"""
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import redis as redislib

import db as dbmod

DB_PATH = "redis_idem.db"
PORT = 18110
REDIS_URL = "redis://127.0.0.1:17001/0"
REDIS_BIN = os.path.join(os.path.dirname(__import__("redislite").__file__), "bin", "redis-server")
STOCK = 10
DUP = 20


def wait_port(port, timeout=20.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.15)
    return False


async def fire():
    async with httpx.AsyncClient(timeout=30) as client:
        async def one():
            r = await client.post(
                f"http://127.0.0.1:{PORT}/seckill",
                json={"request_id": "dup-key-001", "user_id": 7, "sku_id": 1},
            )
            return r.json()["code"]

        return await asyncio.gather(*(one() for _ in range(DUP)))


def main():
    rp = subprocess.Popen([REDIS_BIN, "--port", "17001", "--save", "", "--appendonly", "no"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          start_new_session=True)
    try:
        for _ in range(60):
            try:
                rr = redislib.Redis.from_url(REDIS_URL, decode_responses=True)
                rr.ping()
                break
            except Exception:
                time.sleep(0.15)
        rr.set("seckill:stock:1", STOCK)
        rr.delete("seckill:dedup:1", "seckill:orders")

        dbmod.init(DB_PATH, STOCK)
        env = dict(os.environ, DB_PATH=DB_PATH, REDIS_URL=REDIS_URL)
        sv = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app_redis:app", "--host", "127.0.0.1",
             "--port", str(PORT), "--workers", "4", "--log-level", "warning"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            assert wait_port(PORT), "server not up"
            time.sleep(0.8)
            codes = asyncio.run(fire())
            stable, last = 0, -1
            for _ in range(60):
                cnt = dbmod.snapshot(DB_PATH)["orders"]
                stable = stable + 1 if cnt == last else 0
                last = cnt
                if stable >= 3:
                    break
                time.sleep(0.1)
            snap = dbmod.snapshot(DB_PATH)
            result = {
                "stock": STOCK,
                "concurrent_same_request_id": DUP,
                "code_0_success": codes.count(0),
                "code_3_duplicate": codes.count(3),
                "remaining": int(rr.get("seckill:stock:1")),
                "orders": snap["orders"],
                "passed": codes.count(0) == 1 and codes.count(3) == DUP - 1
                          and int(rr.get("seckill:stock:1")) == STOCK - 1
                          and snap["orders"] == 1,
            }
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            try:
                os.killpg(os.getpgid(sv.pid), signal.SIGTERM)
                sv.wait(timeout=8)
            except Exception:
                pass
    finally:
        try:
            os.killpg(os.getpgid(rp.pid), signal.SIGTERM)
        except Exception:
            pass


if __name__ == "__main__":
    main()
