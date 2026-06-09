# DbspNet — q4 spine merge-probe gate

End-to-end test of the spine merge-probe (`docs/design-row-representation.md` §8) on Nexmark q4 — *average closing price by category* (join → per-group MAX → outer AVG), the step-bound query whose cost lives in the two operators the merge probe touches. The whole q4 pipeline runs as a `W`-replica `ParallelCircuit` in three configurations; the **step** phase is timed apart from ingest (split) and egest (gather), since the merge probe only moves step work:

- **flat** — the `TraceFamily.Flat` default (dictionary-backed traces).
- **spine·point** — `TraceFamily.Spine` with the merge probe *forced off* (`ForcePointProbe`): the LSM substrate, still per-key point-probing. Isolates the cost/benefit of the substrate itself.
- **spine·merge** — `TraceFamily.Spine` with the merge probe live. Isolates the merge probe's contribution on top of spine·point.
- **spine·point·staged / spine·merge·staged** — the same two, with the per-tick **memtable** enabled (flush threshold 8,192 keys, docs §9.7): each tick's delta is an in-place dictionary merge that flushes to a sorted batch only every few ticks, instead of a fresh batch build per tick. This targets the §8.3 finding that the substrate's loss to flat was the per-tick build, not the probe — so *staged vs un-staged* is staging's contribution, and *spine·merge·staged vs flat* is the real question.

Stream: 600,000 events, W=24, median step throughput of 3 run(s) after one warmup. Host: .NET 10.0.8, 24 cores.

The merge probe ships as the typed default only if **spine·merge beats flat**. If it does not, the operator-level win (see `join-probe-bench.md` / `aggregate-probe-bench.md`) does not yet survive the surrounding per-tick exchange/build/integrate cost — the substrate needs cross-operator sharing before the flip pays.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ vs flat | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|--------------:|------------:|------------:|
| flat | 77.3 | 525.9 | 1,140,808 | 1.00× | 0.5 | 10 |
| spine·point | 75.0 | 847.3 | 708,159 | 0.62× | 0.7 | 10 |
| spine·merge | 129.0 | 623.4 | 962,470 | 0.84× | 0.5 | 10 |
| spine·point·staged | 79.0 | 429.5 | 1,396,948 | 1.22× | 0.6 | 10 |
| spine·merge·staged | 99.8 | 439.1 | 1,366,385 | 1.20× | 0.5 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ vs flat | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|--------------:|------------:|------------:|
| flat | 85.5 | 402.0 | 1,492,359 | 1.00× | 0.6 | 10 |
| spine·point | 64.3 | 633.9 | 946,553 | 0.63× | 0.7 | 10 |
| spine·merge | 56.8 | 556.3 | 1,078,532 | 0.72× | 0.6 | 10 |
| spine·point·staged | 129.5 | 441.2 | 1,359,857 | 0.91× | 0.8 | 10 |
| spine·merge·staged | 67.5 | 493.4 | 1,216,151 | 0.81× | 0.6 | 10 |

**Reading it.** *Step↑ vs flat* is the headline gate: > 1.0× means the spine config out-steps the flat default. *spine·merge vs spine·point* attributes the merge probe's share. Expect the gap to widen with batch size (wider D → the merge skips more whole-row hashing); at the small batch, ticks approach the `D == 1` point-probe guard, so spine·merge and spine·point converge.

