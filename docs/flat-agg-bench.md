# DbspNet — flat aggregate lazy merge-view

Host: .NET 10.0.9, 24 cores.

## Flat aggregate — eager rebuild vs lazy merge-view

End-to-end A/B for docs/design-row-representation.md §14.9. The flat `IncrementalAggregateOp` is driven through compiled SQL (`SELECT auction, MAX(price) AS hi, COUNT(*) AS n FROM bids GROUP BY auction`) over 100 auctions, each growing by one new wide bid row per tick across K ticks. `MAX(price)` keeps the inner value rows wide (blocks `NarrowAggregateInput`) and is incremental (probes only the delta). **eager** forces today's `beforeGroup + groupDelta` rebuild (re-hashes the whole group every tick, O(K²) per group); **lazy** is the new `LazyMergeMultiset` view (probes the delta, O(K) per group). Times are median ms for the whole K-tick step loop (compile excluded); **Speedup** = eager/lazy (>1 = lazy wins). Outputs are verified identical.

| K (final group size) | rows/tick | eager | lazy | Speedup |
|---------------------:|----------:|------:|-----:|--------:|
|                  128 |       100 |   203.98 ms |    23.66 ms | 8.62× |
|                  512 |       100 |   555.94 ms |   120.09 ms | 4.63× |
|                 2048 |       100 |    13.79 s |   715.19 ms | 19.3× |

