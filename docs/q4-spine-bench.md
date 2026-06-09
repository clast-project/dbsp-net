# DbspNet — q4 spine merge-probe gate

End-to-end test of the spine merge-probe (`docs/design-row-representation.md` §8) on Nexmark q4 — *average closing price by category* (join → per-group MAX → outer AVG), the step-bound query whose cost lives in the two operators the merge probe touches. The whole q4 pipeline runs as a `W`-replica `ParallelCircuit` in three configurations; the **step** phase is timed apart from ingest (split) and egest (gather), since the merge probe only moves step work:

- **flat** — the `TraceFamily.Flat` default (dictionary-backed traces).
- **spine·point** — `TraceFamily.Spine` with the merge probe *forced off* (`ForcePointProbe`): the LSM substrate, still per-key point-probing. Isolates the cost/benefit of the substrate itself.
- **spine·merge** — `TraceFamily.Spine` with the merge probe live. Isolates the merge probe's contribution on top of spine·point.

Stream: 1,000,000 events, W=24, median step throughput of 3 run(s) after one warmup. Host: .NET 10.0.8, 24 cores.

The merge probe ships as the typed default only if **spine·merge beats flat**. If it does not, the operator-level win (see `join-probe-bench.md` / `aggregate-probe-bench.md`) does not yet survive the surrounding per-tick exchange/build/integrate cost — the substrate needs cross-operator sharing before the flip pays.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ vs flat | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|--------------:|------------:|------------:|
| flat | 141.1 | 693.3 | 1,442,345 | 1.00× | 0.6 | 10 |
| spine·point | 132.8 | 1706.1 | 586,140 | 0.41× | 0.6 | 10 |
| spine·merge | 136.8 | 1643.0 | 608,627 | 0.42× | 0.6 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ vs flat | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|--------------:|------------:|------------:|
| flat | 169.6 | 626.2 | 1,596,868 | 1.00× | 0.7 | 10 |
| spine·point | 202.9 | 1038.6 | 962,815 | 0.60× | 0.7 | 10 |
| spine·merge | 221.1 | 923.8 | 1,082,509 | 0.68× | 0.6 | 10 |

**Reading it.** *Step↑ vs flat* is the headline gate: > 1.0× means the spine config out-steps the flat default. *spine·merge vs spine·point* attributes the merge probe's share. Expect the gap to widen with batch size (wider D → the merge skips more whole-row hashing); at the small batch, ticks approach the `D == 1` point-probe guard, so spine·merge and spine·point converge.

