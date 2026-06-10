# DbspNet — parallel step decomposition (movement vs coordination vs op)

Each parallel-circuit **step** is split, per worker, into **split** (bucket this shard's rows by `hash(key)%W`), **wait** (idle at the exchange barrier — waiting for the slowest splitter + barrier latency), **gather** (rebuild the post-shuffle indexed Z-set, re-hashing full rows) and **op** (the residual operator compute: join / aggregate / TOP-K). The per-phase figures are the **mean per-step ms across the W workers**; **Ctrl** is the controller's real per-step wall clock (= Σ_tick max-worker step — what actually bounds throughput). *Move%* = (split+gather)/step, *Wait%* = wait/step, *Op%* = op/step. **Strag** = Ctrl / mean-Step — the barrier's straggler tax (1.00 = no tax; >1 = the per-tick slowest worker drags the rest). *Imbal* = max-busy / mean-busy, busy = step−wait, the *persistent* per-worker work skew.

Stream: 1,000,000 events, batch 10k. Host: .NET 10.0.9, 24 logical cores. One warmup pass then one profiled pass per (query, W). **Single-pass — read trends, not third-digit cell deltas.**

## q18

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 14.29 | 14.28 | 0.00 | 0.00 | 0.00 | 14.28 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 4 | 8.58 | 8.04 | 1.46 | 0.38 | 0.46 | 5.74 | 24% | 5% | 71% | 1.07 | 1.08 | 1.66× | 2.49× |
| 8 | 6.76 | 6.04 | 0.11 | 0.22 | 0.49 | 5.22 | 10% | 4% | 86% | 1.12 | 1.05 | 2.12× | 2.74× |
| 12 | 4.86 | 3.81 | 0.12 | 0.22 | 0.25 | 3.22 | 10% | 6% | 84% | 1.27 | 1.13 | 2.94× | 4.43× |
| 16 | 6.68 | 5.59 | 0.11 | 0.88 | 0.64 | 3.96 | 13% | 16% | 71% | 1.20 | 1.17 | 2.14× | 3.60× |
| 20 | 5.12 | 3.78 | 0.17 | 0.25 | 0.17 | 3.20 | 9% | 7% | 85% | 1.35 | 1.19 | 2.79× | 4.47× |
| 24 | 4.08 | 2.81 | 0.18 | 0.59 | 0.29 | 1.75 | 17% | 21% | 62% | 1.45 | 1.38 | 3.50× | 8.16× |

## q4

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 17.81 | 17.80 | 0.00 | 0.00 | 0.00 | 17.80 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 4 | 7.54 | 7.39 | 1.84 | 0.45 | 0.45 | 4.64 | 31% | 6% | 63% | 1.02 | 1.01 | 2.36× | 3.84× |
| 8 | 6.04 | 5.89 | 0.07 | 0.85 | 1.15 | 3.82 | 21% | 14% | 65% | 1.03 | 1.02 | 2.95× | 4.66× |
| 12 | 5.59 | 5.38 | 0.41 | 1.05 | 0.64 | 3.28 | 20% | 19% | 61% | 1.04 | 1.06 | 3.19× | 5.43× |
| 16 | 5.06 | 4.65 | 0.08 | 1.68 | 0.58 | 2.31 | 14% | 36% | 50% | 1.09 | 1.19 | 3.52× | 7.72× |
| 20 | 4.74 | 4.45 | 0.04 | 1.58 | 0.21 | 2.61 | 6% | 36% | 59% | 1.07 | 1.24 | 3.76× | 6.82× |
| 24 | 5.04 | 4.68 | 0.06 | 1.89 | 0.17 | 2.57 | 5% | 40% | 55% | 1.08 | 1.14 | 3.53× | 6.93× |

## q19

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 23.74 | 23.74 | 0.00 | 0.00 | 0.00 | 23.74 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 4 | 9.61 | 8.71 | 0.35 | 0.08 | 0.39 | 7.89 | 9% | 1% | 91% | 1.10 | 1.03 | 2.47× | 3.01× |
| 8 | 7.63 | 6.58 | 0.52 | 0.77 | 0.22 | 5.07 | 11% | 12% | 77% | 1.16 | 1.06 | 3.11× | 4.68× |
| 12 | 6.12 | 5.07 | 0.23 | 0.38 | 0.20 | 4.27 | 8% | 7% | 84% | 1.21 | 1.09 | 3.88× | 5.56× |
| 16 | 5.19 | 3.96 | 0.05 | 0.25 | 0.17 | 3.49 | 6% | 6% | 88% | 1.31 | 1.06 | 4.58× | 6.80× |
| 20 | 6.03 | 4.34 | 0.12 | 0.26 | 0.11 | 3.85 | 5% | 6% | 89% | 1.39 | 1.14 | 3.94× | 6.16× |
| 24 | 4.67 | 3.33 | 0.11 | 0.31 | 0.33 | 2.58 | 13% | 9% | 77% | 1.40 | 1.12 | 5.09× | 9.21× |

**Reading it.** Per-step phase ms should fall ~`1/W` if that phase parallelises. A high and *rising* **Wait%** / **Strag** with W means the ceiling is coordination: the per-step/per-exchange barrier pays for the slowest worker each tick, and as W grows each worker's 10k/W-row slice shrinks so the relative variance — and the idle — grows. A flat **Gather**/**Split** that refuses to shrink `1/W` would mean the wide-row movement is bandwidth-bound; an **Op** that scales cleanly while *Ctrl* does not confirms the gap is coordination, not the operator. **Imbal ≫ 1** is *persistent* skew (one worker always heavier — rebalance the hash); **Strag ≫ 1 with Imbal ≈ 1** is *per-tick* straggling (rotating unlucky worker + barrier) — only coarser ticks or fewer barriers help.

> **Host caveat.** This box is an i9-12900K — a *hybrid* 8 P-core (16 threads) + 8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land on the slower E-cores and become *permanent* stragglers the barrier waits on every tick, so the W>16 rows conflate this heterogeneity with the structural barrier tax. On a homogeneous server CPU the W>16 degradation would be milder; the structural trend (barrier tax rising with W at fine ticks) is the portable finding.

