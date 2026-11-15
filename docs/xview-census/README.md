# Exchange census of the ivm-bench program (W=12), 2026-11-15

Two questions about the parallel TPC-DI program's 138 exchanges. **Both are closed.**

## 1. Cross-view partition passthrough: dead (compile-only census)

`XViewPartitionCensus.cs.txt` compiles the ivm-bench program the way the server does
(`numericStringCoercion`, `NullCollation.Low`, `NumericDialect.Feldera`,
`TryCompileParallel(workers: 12)`, broadcast ≤ 1,000 rows at SF=3 counts). It counts exchanges per
view (`baseline-w12-20261115.txt`): 138 among 419 operators in replica 0.

- Sharing the partition map across views removes **0 of 138**. 49 of 50 views end in a projection,
  and a projection drops partition info.
- Adding a bare-column projection remap (`experiment.diff`) removes 2 (138 → 136). Join-side
  elision would remove about 2 more (→ ~134).

Don't revive it.

## 2. Letting window sites skip exchanges: not worth building (measured)

The census's one lever was inside `dim_customer`. One window runs `PARTITION BY customer_id`, then
22 windows run `MAX(..) OVER (PARTITION BY customer_id, x_grp)`, and every window site exchanges
unconditionally. By the subset rule, 22 of the view's 25 exchanges could be skipped.

**Measured cost: 0.5–0.6% of the batch-1 step.** The decision rule set 5% as the stop line.

**Setup.**
- `run1.sh` + `prof1.sh`: IvmMultiBatch at W=12 on local SF=3 data, as the ivm-bench server runs
  it. That means incremental output, broadcast ≤ 1,000, size input order,
  `DOTNET_gcServer=1`, `DOTNET_gcConcurrent=0` and `DOTNET_GCDynamicAdaptationMode=0`.
- Profiling: `DBSPNET_PROFILE=1 DBSPNET_PROFILE_TOP=100000 DBSPNET_PROFILE_VIEW=dim_customer`.
- Binary: `99ecf1c` plus the profiler change, which lists every exchange's barrier wait.
- Runs: 6 (`window-exchanges-w12-20261115.txt`, from `summarize-window-exchanges.py`).

Median over the 6 runs:

| quantity | value |
|---|--:|
| batch-1 engine step (wall) | 13,004 ms |
| the 22 skippable `ExchangeOp`s, CPU summed over 12 workers | 885 ms |
| … of which barrier wait | 677 ms (76%) |
| … as wall (sum ÷ 12) | **74 ms = 0.6% of step** (range 0.5–0.6%) |
| all 25 `dim_customer` exchanges | 0.8% of step |
| `dim_customer`'s 23 `WindowAggregate`s | 2.1% of step |
| whole `dim_customer` view | 3.2% of step |
| allocation of the 22 exchanges | 28.7 MiB of 62.7 GiB (0.04%) |

**Why it's small.**
- `dim_customer` has only **6,440 rows** at SF=3. Each exchange moves 6,440 rows, about 1.3 MiB.
- Each exchange costs ~30 ms of worker CPU, about 2.5 ms of wall per barrier, and three quarters
  of that is barrier wait.
- Most of the wait is imbalance in the window operator just before it. Skipping the exchange
  doesn't remove that imbalance; the tick's final barrier would absorb it. So 0.6% is an upper
  bound on the gain.
- The per-barrier cost is roughly fixed while the row work grows with scale, so at SF10 (GCI) the
  share would be smaller still.

The subset-rule design (reshard placement as the upstream hash, the chain-root partition record,
snapshot fingerprint change) is not written. Reopen it only if a workload has a long window chain
over a **large** partitioned input; this one doesn't.
