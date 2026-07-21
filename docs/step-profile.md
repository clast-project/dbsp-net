# DbspNet — parallel step decomposition (movement vs coordination vs op)

Each parallel-circuit **step** is split, per worker, into **split** (bucket this shard's rows by `hash(key)%W`), **wait** (idle at the exchange barrier — waiting for the slowest splitter + barrier latency), **gather** (rebuild the post-shuffle indexed Z-set, re-hashing full rows) and **op** (the residual operator compute: join / aggregate / TOP-K). The per-phase figures are the **mean per-step ms across the W workers**; **Ctrl** is the controller's real per-step wall clock (= Σ_tick max-worker step — what actually bounds throughput). *Move%* = (split+gather)/step, *Wait%* = wait/step, *Op%* = op/step. **Strag** = Ctrl / mean-Step — the barrier's straggler tax (1.00 = no tax; >1 = the per-tick slowest worker drags the rest). *Imbal* = max-busy / mean-busy, busy = step−wait, the *persistent* per-worker work skew.

Stream: 1,000,000 events, batch 10k. Host: .NET 10.0.1, 10 logical cores. One warmup pass then one profiled pass per (query, W). **Single-pass — read trends, not third-digit cell deltas.**

## q5

| W | Ctrl | Step | Split | Wait | Gather | Op | Move% | Wait% | Op% | Strag | Imbal | Ctrl↓ vs W1 | Op↓ vs W1 |
|--:|-----:|-----:|------:|-----:|-------:|---:|------:|------:|----:|------:|------:|------------:|----------:|
| 1 | 70.01 | 70.00 | 0.00 | 0.00 | 0.00 | 70.00 | 0% | 0% | 100% | 1.00 | 1.00 | 1.00× | 1.00× |
| 2 | 42.13 | 41.90 | 2.03 | 2.25 | 2.66 | 34.95 | 11% | 5% | 83% | 1.01 | 1.04 | 1.66× | 2.00× |
| 4 | 26.46 | 26.06 | 0.55 | 2.33 | 1.75 | 21.42 | 9% | 9% | 82% | 1.02 | 1.05 | 2.65× | 3.27× |
| 10 | 62.75 | 62.00 | 1.97 | 7.91 | 6.09 | 46.03 | 13% | 13% | 74% | 1.01 | 1.02 | 1.12× | 1.52× |

**Reading it.** Per-step phase ms should fall ~`1/W` if that phase parallelises. A high and *rising* **Wait%** / **Strag** with W means the ceiling is coordination: the per-step/per-exchange barrier pays for the slowest worker each tick, and as W grows each worker's 10k/W-row slice shrinks so the relative variance — and the idle — grows. A flat **Gather**/**Split** that refuses to shrink `1/W` would mean the wide-row movement is bandwidth-bound; an **Op** that scales cleanly while *Ctrl* does not confirms the gap is coordination, not the operator. **Imbal ≫ 1** is *persistent* skew (one worker always heavier — rebalance the hash); **Strag ≫ 1 with Imbal ≈ 1** is *per-tick* straggling (rotating unlucky worker + barrier) — only coarser ticks or fewer barriers help.

> **Host caveat.** This box is an i9-12900K — a *hybrid* 8 P-core (16 threads) + 8 E-core (8 threads) part, 24 logical. Past W≈16 some workers land on the slower E-cores and become *permanent* stragglers the barrier waits on every tick, so the W>16 rows conflate this heterogeneity with the structural barrier tax. On a homogeneous server CPU the W>16 degradation would be milder; the structural trend (barrier tax rising with W at fine ticks) is the portable finding.

