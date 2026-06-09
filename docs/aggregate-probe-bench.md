# DbspNet — aggregate merge-probe

Host: .NET 10.0.8, 24 cores.

## Aggregate merge-probe — point probe vs galloping merge at the operator

Operator-level gate for docs/design-row-representation.md §8 (the IMultiset increment): the whole `SpineIncrementalAggregateOp.Step` is timed with the per-key `GroupFor` + Z-set rebuild (`ForcePointProbe`, today's baseline — two hashing passes over the after-group) versus the batched `GroupForManySorted` merge feeding a `SortedRunMultiset` (no rehash). Each tick updates `D` existing groups (group size 4, wide values); ticks alternate add/retract so state stays bounded. Times are median ns per **Step**; **Speedup** is point/merge (>1 = merge wins). Aggregates only touch keys in their delta, so every probe hits an existing group (no absent case).

| N | D=1 | D=8 | D=64 | D=512 | D=4096 |
|--:|----:|----:|-----:|------:|-------:|
| 1,000 | 0.90× | 1.70× | 2.39× | 2.45× | 5.47× |
| 100,000 | 1.32× | 1.48× | 2.40× | 2.33× | 2.38× |
| 1,000,000 | 1.00× | 1.79× | 2.00× | 2.21× | 2.39× |

Reading: the merge wins on multi-key ticks (`D > 1`) by skipping the two whole-group hashing passes the Z-set rebuild pays; `D == 1` keeps the point probe (the trace-level soft spot), so that column is ~1.0× by construction. The win is the after-group build cost the IMultiset abstraction removes — the aggregators are unchanged.

