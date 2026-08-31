# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.9, 24 cores, `Microsoft Windows 10.0.26200`.

## Fraud detection — rolling-window features

Card `transactions` joined to `customers`, computing per-customer rolling 1-day / 7-day / 30-day transaction **count** and **sum** as real-time ML features (Feldera's documented fraud-detection use case). Three distinct `RANGE … INTERVAL` window frames feed off one join. We load a transaction history, then measure the steady-state cost of scoring **one** new transaction (`Insert` + `Step`) — the latency that matters when fraud must be caught per swipe.

| History txns | Per-event latency | Throughput (events/s) |
|-------------:|------------------:|----------------------:|
| 10,000 | 106.80 µs | 50,628 |

The per-event latency is the headline: once the history is loaded, scoring an additional transaction touches only the affected customer's window state. It stays in the tens-of-µs range across a 50× growth in history (the slow drift reflects larger trace / window-state working sets and allocator pressure, not a full rescan) — the incremental property that makes DBSP suitable for per-transaction fraud scoring. Compare this against a from-scratch recompute of the same feature view.

### Parallel scaling

The full rolling-window feature view compiles to a parallel circuit — each `cust_id` partition's window state is co-located on one worker by an exchange on the PARTITION BY key. The table measures the whole view (join + the three `RANGE` window frames) at W=1 vs W. W>1 output is cross-checked against the W=1 replica run.

Feature view `SELECT t.txn_id, t.cust_id, c.zip,             COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS cnt_1d,             SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW)  AS sum_1d,             COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS cnt_7d,             SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)  AS sum_7d,             COUNT(*)        OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS cnt_30d,             SUM(t.amount)   OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS sum_30d           FROM transactions t JOIN customers c ON t.cust_id = c.id`:

| History txns | W=1 (events/s) | W=4 (events/s) | Speedup | Status |
|-------------:|---------------:|---------------:|--------:|:-------|
| 10,000 | 52,061 | 121,075 | 2.33× | ok |

