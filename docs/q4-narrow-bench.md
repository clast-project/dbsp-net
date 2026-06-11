# DbspNet — q4 non-linear aggregate input narrowing gate (§18)

W>1 in-`Step` A/B of narrowing the inner `MAX(price) GROUP BY auction` input to `{auction_id, category, price}` (vs the full ~17-column join row). Both configs are the `TraceFamily.Flat` default; the only difference is whether the q4 plan was optimized with non-linear narrowing enabled.

Stream: 1,000,000 events, W=8, median **step** throughput of 3 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores. The narrowed output is cross-checked identical to the full-row output (insert-only ⇒ sound).

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·full | 218.4 | 579.1 | 1,726,767 | 1.00× | 0.7 | 10 |
| flat·narrow | 248.2 | 424.1 | 2,357,952 | 1.37× | 0.5 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·full | 228.4 | 564.8 | 1,770,551 | 1.00× | 0.5 | 10 |
| flat·narrow | 182.9 | 462.1 | 2,164,165 | 1.22× | 0.8 | 10 |

**Reading it.** *Step↑* is full / narrowed on the step phase. Narrowing is in-`Step` (it shrinks the rows the inner aggregate hashes/stores), so per §16.11 the W=1 win is expected to translate to W>1 rather than be Amdahl-eaten the way the out-of-`Step` output-boundary lever was.

