# DbspNet — q19 narrow-key partitioned TOP-K gate (§22)

W>1 in-`Step` A/B of keying the partitioned TOP-K trace by a narrow `{order, wide row}` key (`PartitionedTopKNarrowingMode`) vs whole-row keying. Both configs are the `TraceFamily.Flat` default. Narrow output is cross-checked identical to whole-row output.

Stream: 1,000,000 events, W=8, median **step** throughput of 5 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 155.1 | 642.9 | 1,555,499 | 1.00× | 4.1 | 8,706 |
| flat·narrow | 181.3 | 448.8 | 2,228,061 | 1.43× | 3.6 | 8,706 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 135.7 | 774.0 | 1,291,967 | 1.00× | 21.2 | 62,158 |
| flat·narrow | 171.4 | 423.8 | 2,359,516 | 1.83× | 70.1 | 62,158 |

**Reading it.** *Step↑* is whole-row / narrow on the step phase. The narrow key is in-`Step` (it shrinks what the TOP-K op hashes/stores), so per §16.11 a W=1 win is expected to translate to W>1 — but per §16.10 it is Amdahl-diluted by the exchange/coordination share (larger for q18 than q19).

