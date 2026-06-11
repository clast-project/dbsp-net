# DbspNet — parallel step decomposition (movement vs coordination vs op)

Each parallel-circuit **step** is split, per worker, into **split** (bucket this shard's rows by `hash(key)%W`), **wait** (idle at the exchange barrier — waiting for the slowest splitter + barrier latency), **gather** (rebuild the post-shuffle indexed Z-set, re-hashing full rows) and **op** (the residual operator compute: join / aggregate / TOP-K). The per-phase figures are the **mean per-step ms across the W workers**; **Ctrl** is the controller's real per-step wall clock (= Σ_tick max-worker step — what actually bounds throughput). *Move%* = (split+gather)/step, *Wait%* = wait/step, *Op%* = op/step. **Strag** = Ctrl / mean-Step — the barrier's straggler tax (1.00 = no tax; >1 = the per-tick slowest worker drags the rest). *Imbal* = max-busy / mean-busy, busy = step−wait, the *persistent* per-worker work skew.

Stream: 1,000,000 events, batch 10k. Host: .NET 10.0.9, 24 logical cores. One warmup pass then one profiled pass per (query, W). **Single-pass — read trends, not third-digit cell deltas.**

## q18

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 8 | 7.17 | 6.59 | 0.61 | 0.56 | 1.69 | 3.73 | 35% | 8% | 57% | 1.09 | 1.08 | 0.00× | 0.00× |
| 12 | 4.93 | 3.36 | 0.33 | 0.50 | 0.24 | 2.30 | 17% | 15% | 68% | 1.47 | 1.34 | 0.00× | 0.00× |
| 16 | 3.61 | 3.00 | 0.09 | 1.01 | 0.24 | 1.66 | 11% | 34% | 55% | 1.21 | 1.25 | 0.00× | 0.00× |

**Reading it.** Per-step phase ms should fall ~`1/W` if that phase parallelises. A high and *rising* **Wait%** / **Strag** with W means the ceiling is coordination: the per-step/per-exchange barrier pays for the slowest worker each tick, and as W grows each worker's 10k/W-row slice shrinks so the relative variance — and the idle — grows. A flat **Gather**/**Split** that refuses to shrink `1/W` would mean the wide-row movement is bandwidth-bound; an **Op** that scales cleanly while *Ctrl* does not confirms the gap is coordination, not the operator. **Imbal ≫ 1** is *persistent* skew (one worker always heavier — rebalance the hash); **Strag ≫ 1 with Imbal ≈ 1** is *per-tick* straggling (rotating unlucky worker + barrier) — only coarser ticks or fewer barriers help.

> **Host caveat.** This box is an i9-12900K — a *hybrid* 8 P-core (16 threads) + 8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land on the slower E-cores and become *permanent* stragglers the barrier waits on every tick, so the W>16 rows conflate this heterogeneity with the structural barrier tax. On a homogeneous server CPU the W>16 degradation would be milder; the structural trend (barrier tax rising with W at fine ticks) is the portable finding.

