# DbspNet — q4 cross-tick delta-pooling gate (§20)

W>1 in-`Step` A/B of reusing the join/aggregate output builders across ticks (`DeltaPoolMode`) vs a fresh builder each `Step`. Both configs are the `TraceFamily.Flat` default. Pooled output is cross-checked identical.

Stream: 1,000,000 events, W=8, median **step** throughput of 5 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·unpooled | 179.9 | 658.0 | 1,519,832 | 1.00× | 0.7 | 10 |
| flat·pooled | 168.5 | 578.2 | 1,729,512 | 1.14× | 0.7 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·unpooled | 206.4 | 654.4 | 1,528,045 | 1.00× | 0.6 | 10 |
| flat·pooled | 304.5 | 509.2 | 1,963,940 | 1.29× | 0.5 | 10 |

**Reading it.** *Step↑* is unpooled / pooled on the step phase. Pooling is in-`Step` but only removes the *steady* builder backing that §16.8 pre-sizing still re-allocates — a thin term, so a small step move is expected.

