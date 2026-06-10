# DbspNet — q4 flat aggregate lazy merge-view gate

Query-level A/B of the flat aggregate lazy merge-view (`docs/design-row-representation.md` §14.10) on Nexmark q4 — *average closing price by category* (join → per-group `MAX` → outer `AVG`). Both configs are the `TraceFamily.Flat` default; the only difference is the aggregate's post-delta group representation:

- **flat·eager** — the old `afterGroup = beforeGroup + groupDelta` rebuild (re-hashes the whole per-auction group every tick).
- **flat·lazy** — the `LazyMergeMultiset` view; q4's typed incremental `MAX` probes only the delta, so the view removes the aggregate's only O(K)-per-tick term (asymptotic O(K²)→O(K)).

Stream: 1,000,000 events, W=24, median **step** throughput of 5 run(s) after one warmup, timed apart from split/gather. Host: .NET 10.0.9, 24 cores. The lazy output is cross-checked identical to eager.

## Batch = 10,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·eager | 165.7 | 622.4 | 1,606,776 | 1.00× | 0.6 | 10 |
| flat·lazy | 212.1 | 487.6 | 2,050,696 | 1.28× | 0.5 | 10 |

## Batch = 100,000 events

| Config | Split (ms) | Step (ms) | Step events/s | Step↑ | Gather (ms) | Output rows |
|:-------|-----------:|----------:|--------------:|------:|------------:|------------:|
| flat·eager | 241.4 | 610.6 | 1,637,853 | 1.00× | 0.4 | 10 |
| flat·lazy | 320.8 | 412.5 | 2,424,493 | 1.48× | 0.5 | 10 |

**Reading it.** *Step↑* is flat·eager / flat·lazy on the step phase. The operator-level `flatagg` gate isolated 4.6–19×; here that win is diluted by the join, exchange, and outer AVG that the lazy view does not touch, so the query-level number is the realistic gain. The win grows with batch size (wider per-replica ticks → larger per-auction groups rebuilt per tick under eager).

