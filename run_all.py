"""跑完全部对照实验，结果写入 results.json。

三方对照：
  naive      SQLite 先查后改（常见 CRUD 写法）
  db_atomic  SQLite 原子扣减 + 事务 + BUSY 重试 + 异步批量落库
  redis      Redis + Lua 原子预扣 + Set 幂等 + List 队列异步落库
"""
import json
import os
import subprocess
import sys
import time

import redis as redislib

import db as dbmod

REDIS_PORT = 17001
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"
REDIS_BIN = os.path.join(
    os.path.dirname(__import__("redislite").__file__), "bin", "redis-server"
)

RUNS = [
    # 场景 A：库存 100，1500 请求 —— 验证是否超卖
    ("A-oversell", "app_naive:app", "naive_a.db", 100, 1500, 300, 18101, False),
    ("A-oversell", "app_db_atomic:app", "dbatomic_a.db", 100, 1500, 300, 18102, False),
    ("A-oversell", "app_redis:app", "redis_a.db", 100, 1500, 300, 18105, True),
    # 场景 B：库存 2000，2000 请求全部成功 —— 比较吞吐与尾延迟
    ("B-throughput", "app_naive:app", "naive_b.db", 2000, 2000, 300, 18103, False),
    ("B-throughput", "app_db_atomic:app", "dbatomic_b.db", 2000, 2000, 300, 18104, False),
    ("B-throughput", "app_redis:app", "redis_b.db", 2000, 2000, 300, 18106, True),
]


def start_redis():
    proc = subprocess.Popen(
        [REDIS_BIN, "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    for _ in range(60):
        try:
            redislib.Redis.from_url(REDIS_URL).ping()
            return proc
        except Exception:
            time.sleep(0.15)
    proc.kill()
    raise RuntimeError("redis did not start")


def main():
    rproc = start_redis()
    results = []
    try:
        for tag, app, dbfile, stock, total, conc, port, use_redis in RUNS:
            print(f"==> {tag} {app} stock={stock} total={total} conc={conc}", flush=True)
            cmd = [sys.executable, "run_bench.py", "--app", app, "--db", dbfile,
                   "--stock", str(stock), "--total", str(total), "--conc", str(conc),
                   "--port", str(port), "--tag", tag]
            if use_redis:
                cmd += ["--redis-url", REDIS_URL]
            out = subprocess.run(cmd, capture_output=True, text=True, check=True)
            line = [l for l in out.stdout.strip().splitlines() if l.startswith("{")][-1]
            res = json.loads(line)
            results.append(res)
            print(json.dumps(res, ensure_ascii=False, indent=2), flush=True)
    finally:
        rproc.kill()
    with open("results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\nwritten results.json", flush=True)


if __name__ == "__main__":
    main()
