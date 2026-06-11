# DbspNet — q4 join column-pruning gate (§21)

W>1 in-`Step` A/B of pruning the `auction ⋈ bid` stored join rows to the columns the aggregate/residual/equi-key read (`{id,category,date_time,expires}` and `{auction,price,date_time}`) vs the full ~10/~7-column source rows. Both configs are the `TraceFamily.Flat` default; the only difference is whether the q4 plan was optimized with join column pruning enabled.

Stream: 1,000,000 events, W=8, median **step** throughput of 3 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores. The pruned output is cross-checked identical to the full-row output (unconditionally sound).

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·full | 235.9 | 594.8 | 1,681,316 | 1.00× | 0.8 | 10 |
| flat·prune | 285.8 | 203.1 | 4,923,031 | 2.93× | 0.5 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·full | 174.7 | 620.5 | 1,611,511 | 1.00× | 0.5 | 10 |
| flat·prune | 251.6 | 148.0 | 6,758,076 | 4.19× | 0.7 | 10 |

**Reading it.** *Step↑* is full / pruned on the step phase. Pruning is in-`Step` (it shrinks the rows the join trace hashes/stores on integrate), so per §16.11 the W=1 win is expected to translate to W>1 rather than be Amdahl-eaten the way the out-of-`Step` output-boundary lever was.

