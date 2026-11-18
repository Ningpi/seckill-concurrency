# 高并发秒杀下单：超卖 78 件到 0 的对照实验

同一个下单接口写三个版本，在相同的并发压力下比较**数据正确性**。核心不是"用了 Redis"，而是用可复现的实验证明：为什么先查后改会超卖、原子操作为什么能修好、以及每种方案的代价。

**结果：300 并发 / 1500 请求 / 库存 100 —— 基线超卖 78 件，两个优化版本超卖 0，库存与订单完全一致。**

## 实测结果

环境：3 核 / 6 GB，uvicorn 4 worker，客户端 300 并发。压测客户端与服务端同机（会抢 CPU），因此**不把绝对 QPS 作为结论**，只比较正确性。

### 场景 A：库存 100，1500 个请求

| 版本 | 并发控制 | 成功 | 剩余库存 | 订单数 | 超卖 |
|---|---|---|---|---|---|
| `app_naive.py` | 无（先查后改） | 178 | **-78** | 178 | **78** |
| `app_db_atomic.py` | SQL 原子扣减 | 100 | 0 | 100 | 0 |
| `app_redis.py` | Redis + Lua | 100 | 0 | 100 | 0 |

### 场景 B：库存 2000，2000 个请求全部成功

| 版本 | 成功 | 剩余 | 订单 | 一致性 |
|---|---|---|---|---|
| naive | 2000 | 0 | 2000 | 一致（无超卖压力） |
| db_atomic | 2000 | 0 | 2000 | 一致 |
| redis | 2000 | 0 | 2000 | 一致 |

### 幂等性

`test_idempotent.py`：库存 10，20 个并发请求使用**同一个** `request_id`。

```json
{
  "stock": 10,
  "concurrent_same_request_id": 20,
  "code_0_success": 1,
  "code_3_duplicate": 19,
  "remaining": 9,
  "orders": 1,
  "passed": true
}
```

## 快速开始

依赖 Python 3.10+。不需要预装 Redis —— `redislite` 自带 `redis-server`，脚本会自动拉起。

```bash
pip install -r requirements.txt

python run_all.py          # 三方对照 × 两个场景，约 1–2 分钟，写入 results.json
python test_idempotent.py  # 幂等性验证，输出上方 JSON
```

跑完后 `results.json` 里是每次运行的完整记录（成功/售罄/重复数、P50/P95/P99、库存与订单快照），可以直接和本 README 的表格核对。

单独跑某个版本：

```bash
python -c "import db; db.init('manual.db', 100)"     # 建库并置库存 100
DB_PATH=manual.db python -m uvicorn app_db_atomic:app --port 18200 --workers 4

curl -X POST localhost:18200/seckill \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"r1","user_id":1,"sku_id":1}'
```

返回码：`0` 成功、`2` 售罄、`3` 重复请求、`429` 被限流。

## 目录结构

```
.
├── db.py                  # SQLite 初始化；WAL + busy_timeout + 手动事务
├── app_naive.py           # 基线：先 SELECT 再 UPDATE（check-then-act）
├── app_db_atomic.py       # 原子扣减 + BEGIN IMMEDIATE + BUSY 退避 + 令牌桶 + 队列落库
├── app_redis.py           # Lua 原子预扣 + Set 幂等 + List 队列 + 后台批量落库
├── run_bench.py           # 启动 uvicorn、压测、等待落库稳定、采集快照
├── run_all.py             # 三版本 × 两场景一键对照
├── test_idempotent.py     # 同一 request_id 并发去重验证
└── results.json           # 最近一次实测结果
```

## 三种实现的关键差异

**naive —— 问题复现**
`SELECT remaining` 和 `UPDATE ... - 1` 是两条独立语句，中间没有锁也没有版本校验。并发请求读到同一个 `remaining=1`，各自扣减，库存变负。代码里 `time.sleep(0.002)` 模拟风控/校验耗时的业务间隙，让竞态稳定复现；真实系统窗口更小，但问题同样存在。

**db_atomic —— 单机数据库的正确性方案**
把判断和扣减压进一条语句：

```sql
UPDATE stock SET remaining = remaining - 1 WHERE sku_id = ? AND remaining > 0
```

`rowcount == 0` 即售罄。配合 `BEGIN IMMEDIATE` 提前拿写锁、`SQLITE_BUSY` 指数退避、线程本地连接复用；订单先进内存队列，由后台线程批量 `executemany` 落库，避免每次下单都等一次 fsync。

**redis —— 把原子性挪到数据库之前**
Lua 脚本在 Redis 单线程内一次性完成「读库存 → `SISMEMBER` 判重 → `DECR` → `RPUSH`」，既保证原子性又省掉多次网络往返。Set 负责 `request_id` 幂等，List 做削峰队列，后台按 500 条一批落库。

## 已知限制

- 单机 SQLite + 单进程 Redis，**没有**做分布式锁、库存对账或失败补偿；Redis 版是最终一致，落库失败会丢单。
- 压测客户端与服务端抢同一台机器的 CPU，绝对 QPS 无参考价值，因此只报告并发数、超卖量与一致性。
- 幂等只覆盖 `request_id` 维度，未处理同一用户重复下单的业务规则。
- 未接入监控与链路追踪，"等待订单数连续 3 次稳定" 是脚本里最朴素的收尾判断。

## 可以继续做的方向

1. 用 JMeter / wrk 在独立机器上压，得到有意义的 QPS 曲线。
2. 把库存扣减换成预扣 + 支付确认 + 超时回补的完整状态机。
3. 用 `pytest` 把三个版本的断言固化成回归测试，而不是每次人工看 JSON。
