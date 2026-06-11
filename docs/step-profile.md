# DbspNet — parallel step decomposition (movement vs coordination vs op)

Each parallel-circuit **step** is split, per worker, into **split** (bucket this shard's rows by `hash(key)%W`), **wait** (idle at the exchange barrier — waiting for the slowest splitter + barrier latency), **gather** (rebuild the post-shuffle indexed Z-set, re-hashing full rows) and **op** (the residual operator compute: join / aggregate / TOP-K). The per-phase figures are the **mean per-step ms across the W workers**; **Ctrl** is the controller's real per-step wall clock (= Σ_tick max-worker step — what actually bounds throughput). *Move%* = (split+gather)/step, *Wait%* = wait/step, *Op%* = op/step. **Strag** = Ctrl / mean-Step — the barrier's straggler tax (1.00 = no tax; >1 = the per-tick slowest worker drags the rest). *Imbal* = max-busy / mean-busy, busy = step−wait, the *persistent* per-worker work skew.

Stream: 1,000,000 events, batch 10k. Host: .NET 10.0.9, 24 logical cores. One warmup pass then one profiled pass per (query, W). **Single-pass — read trends, not third-digit cell deltas.**

## q18

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 14.13 | 14.13 | 0.00 | 0.00 | 0.00 | 14.13 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 10 | 5.73 | 4.88 | 1.53 | 1.06 | 0.57 | 1.72 | 43% | 22% | 35% | 1.17 | 1.29 | 2.47× | 8.20× |
| 14 | 3.69 | 3.16 | 0.08 | 0.39 | 0.39 | 2.30 | 15% | 12% | 73% | 1.16 | 1.05 | 3.83× | 6.13× |
| 24 | 6.30 | 4.19 | 0.16 | 1.54 | 0.25 | 2.23 | 10% | 37% | 53% | 1.50 | 1.47 | 2.24× | 6.32× |

## q19

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 23.49 | 23.49 | 0.00 | 0.00 | 0.00 | 23.49 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 10 | 6.03 | 5.15 | 0.16 | 0.29 | 0.40 | 4.29 | 11% | 6% | 83% | 1.17 | 1.08 | 3.89× | 5.48× |
| 14 | 5.71 | 4.96 | 0.52 | 0.15 | 0.17 | 4.12 | 14% | 3% | 83% | 1.15 | 1.02 | 4.11× | 5.70× |
| 24 | 5.03 | 3.51 | 0.31 | 0.52 | 0.16 | 2.52 | 13% | 15% | 72% | 1.43 | 1.19 | 4.67× | 9.32× |

**Reading it.** Per-step phase ms should fall ~`1/W` if that phase parallelises. A high and *rising* **Wait%** / **Strag** with W means the ceiling is coordination: the per-step/per-exchange barrier pays for the slowest worker each tick, and as W grows each worker's 10k/W-row slice shrinks so the relative variance — and the idle — grows. A flat **Gather**/**Split** that refuses to shrink `1/W` would mean the wide-row movement is bandwidth-bound; an **Op** that scales cleanly while *Ctrl* does not confirms the gap is coordination, not the operator. **Imbal ≫ 1** is *persistent* skew (one worker always heavier — rebalance the hash); **Strag ≫ 1 with Imbal ≈ 1** is *per-tick* straggling (rotating unlucky worker + barrier) — only coarser ticks or fewer barriers help.

> **Host caveat.** This box is an i9-12900K — a *hybrid* 8 P-core (16 threads) + 8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land on the slower E-cores and become *permanent* stragglers the barrier waits on every tick, so the W>16 rows conflate this heterogeneity with the structural barrier tax. On a homogeneous server CPU the W>16 degradation would be milder; the structural trend (barrier tax rising with W at fine ticks) is the portable finding.

