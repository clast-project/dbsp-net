# DbspNet — single-core path A/B (structural vs typed W=1)

The comparison's single-core column uses the **structural** `CompiledQuery` (`object?[]`→`StructuralRow`→typed at the scan/sink). The **typed W=1** path (`ParallelTypedCompiledQuery` at one worker) encodes `object?[]`→`ZSet<TRow>` directly — typed ingest, already shipped on the parallel path. `typed/struct` is the prize a single-circuit typed-ingest+egest change would capture for single-core.

Stream: 1,000,000 events, batch 10,000, median of 3 runs after one warmup. Host: .NET 10.0.9, 24 cores.

| Query | structural ev/s | typed W=1 ev/s | typed/struct | struct out | typed out |
|:------|----------------:|---------------:|-------------:|-----------:|----------:|
| q0 | 2,295,409 | 2,645,261 | 1.15× | 0 | 0 |
| q1 | 1,868,243 | 2,059,674 | 1.10× | 0 | 0 |
| q2 | 2,821,386 | 2,594,225 | 0.92× | 0 | 0 |
| q22 | 1,217,748 | 1,412,938 | 1.16× | 0 | 0 |
| q3 | 15,663,547 | 13,330,187 | 0.85× | 0 | 0 |
| q20 | 1,142,259 | 1,140,438 | 1.00× | 0 | 0 |
| q4 | 965,068 | 974,834 | 1.01× | 0 | 0 |
| q9 | 808,692 | 796,529 | 0.98× | 0 | 0 |
| q18 | 545,224 | 588,686 | 1.08× | 0 | 0 |
| q19 | 394,325 | 421,392 | 1.07× | 0 | 0 |

**Reading it.** `typed/struct` > 1 means the structural single circuit is leaving single-core throughput on the table that a typed-ingest+egest single circuit would recover — i.e. that much of the single-core gap is a path choice, not a per-row floor.

