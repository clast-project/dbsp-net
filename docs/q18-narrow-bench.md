# DbspNet — q18 narrow-key partitioned TOP-K gate (§22)

W>1 in-`Step` A/B of keying the partitioned TOP-K trace by a narrow `{order, wide row}` key (`PartitionedTopKNarrowingMode`) vs whole-row keying. Both configs are the `TraceFamily.Flat` default. Narrow output is cross-checked identical to whole-row output.

Stream: 1,000,000 events, W=8, median **step** throughput of 5 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 206.4 | 609.8 | 1,639,781 | 1.00× | 4.6 | 9,200 |
| flat·narrow | 204.1 | 775.7 | 1,289,090 | 0.79× | 4.5 | 9,200 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·wholerow | 133.1 | 837.1 | 1,194,604 | 1.00× | 52.1 | 91,972 |
| flat·narrow | 246.3 | 823.9 | 1,213,685 | 1.02× | 34.5 | 91,972 |

**Reading it.** *Step↑* is whole-row / narrow on the step phase. The narrow key is in-`Step` (it shrinks what the TOP-K op hashes/stores), so per §16.11 a W=1 win is expected to translate to W>1 — but per §16.10 it is Amdahl-diluted by the exchange/coordination share (larger for q18 than q19).

