# DbspNet — q19 narrow-key partitioned TOP-K gate (§22)

W>1 in-`Step` A/B of keying the partitioned TOP-K trace by a narrow `{order, wide row}` key (`PartitionedTopKNarrowingMode`) vs whole-row keying. Both configs are the `TraceFamily.Flat` default. Narrow output is cross-checked identical to whole-row output.

Stream: 1,000,000 events, W=8, median **step** throughput of 3 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 149.5 | 774.4 | 1,291,248 | 1.00× | 6.5 | 8,706 |
| flat·narrow | 179.4 | 518.5 | 1,928,492 | 1.49× | 4.3 | 8,706 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 185.0 | 799.4 | 1,250,895 | 1.00× | 24.1 | 62,158 |
| flat·narrow | 293.7 | 472.7 | 2,115,340 | 1.69× | 23.8 | 62,158 |

**Reading it.** *Step↑* is whole-row / narrow on the step phase. The narrow key is in-`Step` (it shrinks what the TOP-K op hashes/stores), so per §16.11 a W=1 win is expected to translate to W>1 — but per §16.10 it is Amdahl-diluted by the exchange/coordination share (larger for q18 than q19).

