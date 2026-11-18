"""压测脚本：启动 uvicorn（4 worker）-> 并发打点 -> 汇总指标 -> 校验数据一致性。"""
import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections import Counter

import httpx

import db as dbmod


def wait_port(port: int, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.15)
    return False


def pct(values, p):
    if not values:
        return 0.0
    k = max(0, min(len(values) - 1, int(round((p / 100) * (len(values) - 1)))))
    return sorted(values)[k]


async def load(url: str, total: int, conc: int, run_id: str):
    sem = asyncio.Semaphore(conc)
    lat = []
    codes = Counter()
    limits = httpx.Limits(max_connections=conc + 10, max_keepalive_connections=conc + 10)
    async with httpx.AsyncClient(timeout=60.0, limits=limits) as client:
        async def one(i: int):
            async with sem:
                t0 = time.perf_counter()
                try:
                    r = await client.post(
                        url,
                        json={"request_id": f"{run_id}-{i}", "user_id": i % 997 + 1, "sku_id": 1},
                    )
                    lat.append(time.perf_counter() - t0)
                    codes[r.json().get("code")] += 1
                except Exception as exc:  # noqa: BLE001
                    codes[f"exc:{type(exc).__name__}"] += 1

        t0 = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(total)))
        elapsed = time.perf_counter() - t0
    return lat, codes, elapsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--stock", type=int, required=True)
    ap.add_argument("--total", type=int, required=True)
    ap.add_argument("--conc", type=int, default=300)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--redis-url", default="")
    args = ap.parse_args()

    dbmod.init(args.db, args.stock)
    env = dict(os.environ, DB_PATH=args.db, PYTHONUNBUFFERED="1")
    if args.redis_url:
        import redis as redislib

        rr = redislib.Redis.from_url(args.redis_url, decode_responses=True)
        rr.set("seckill:stock:1", args.stock)
        rr.delete("seckill:dedup:1")
        rr.delete("seckill:orders")
        env["REDIS_URL"] = args.redis_url
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", args.app, "--host", "127.0.0.1",
         "--port", str(args.port), "--workers", str(args.workers), "--log-level", "warning"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    try:
        if not wait_port(args.port):
            raise RuntimeError("server did not start")
        time.sleep(1.0)
        lat, codes, elapsed = asyncio.run(
            load(f"http://127.0.0.1:{args.port}/seckill", args.total, args.conc, args.tag)
        )
        # 等待异步落库线程把队列写空（订单数连续 3 次不变即视为稳定）
        stable, last = 0, -1
        for _ in range(80):
            cnt = dbmod.snapshot(args.db)["orders"]
            stable = stable + 1 if cnt == last else 0
            last = cnt
            if stable >= 3:
                break
            time.sleep(0.1)
        snap = dbmod.snapshot(args.db)
        if args.redis_url:
            import redis as redislib

            rr = redislib.Redis.from_url(args.redis_url, decode_responses=True)
            snap["remaining"] = int(rr.get("seckill:stock:1") or -1)
        ok = codes.get(0, 0) + codes.get(3, 0)
        oversell = max(0, ok - args.stock)
        result = {
            "tag": args.tag,
            "app": args.app,
            "stock": args.stock,
            "total_requests": args.total,
            "concurrency": args.conc,
            "workers": args.workers,
            "elapsed_s": round(elapsed, 3),
            "qps": round(args.total / elapsed, 1),
            "latency_ms": {
                "p50": round(pct(lat, 50) * 1000, 1),
                "p95": round(pct(lat, 95) * 1000, 1),
                "p99": round(pct(lat, 99) * 1000, 1),
                "max": round(max(lat) * 1000, 1) if lat else 0,
            },
            "codes": dict(codes),
            "success": codes.get(0, 0),
            "sold_out": codes.get(2, 0),
            "dup": codes.get(3, 0),
            "errors": sum(v for k, v in codes.items() if k not in (0, 2, 3)),
            "remaining": snap["remaining"],
            "orders": snap["orders"],
            "oversell": oversell,
            "sold_equals_orders": codes.get(0, 0) == snap["orders"],
        }
        print(json.dumps(result, ensure_ascii=False))
        return result
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=10)
        except Exception:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except Exception:
                pass


if __name__ == "__main__":
    main()
