# DbspNet — representation/execution decomposition

## Representation-vs-execution decomposition (§17)

Per-tick hot loop (build a delta dictionary of `D` wide-row keys, fold it into retained state, enumerate the changed entries) timed four ways at three key widths. `gen·fresh` is today's model. The successive deltas apportion the per-tuple floor:

- **alloc tax** = `gen·fresh − gen·pooled` (heap throughput: fresh `Dictionary`/tick).
- **abstraction tax** = `gen·pooled − mono·deleg` (ZSet/IZRing/Optional generic wrapper).
- **dispatch tax** = `mono·deleg − mono·inline` (per-row `Func<>` delegate call).
- **compute floor** = `mono·inline` (irreducible wide-key hash + arithmetic).

Stream: 4,000 ticks × 256 rows/tick, median of 7 runs. `ns/row` is per delta row; `B/row` is managed bytes per delta row (`GC.GetAllocatedBytesForCurrentThread`). Host: .NET 10.0.9, 24 cores, Server GC.

| Width | gen·fresh ns | gen·pooled ns | mono·deleg ns | mono·inline ns | alloc | abstraction | dispatch | floor |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| W2 (2 long) | 52.3 (308B) | 19.8 (0B) | 27.6 (0B) | 16.7 (0B) | 32.5 | -7.8 | 10.9 | 16.7 |
| W8 (8 long) | 114.0 (711B) | 59.2 (0B) | 63.0 (0B) | 45.3 (0B) | 54.8 | -3.8 | 17.8 | 45.3 |
| WStr (3 long+str) | 172.9 (442B) | 84.7 (0B) | 91.4 (0B) | 81.7 (0B) | 88.3 | -6.7 | 9.7 | 81.7 |

## Join-trace integrate: wide-stored vs narrow-stored inner multiset (§21)

The term-2 site §18 narrowing cannot reach: `IncrementalJoinOp`'s trace is an `IndexedZSet<joinKey, storedRow>` whose inner `ZSet` is keyed by the **whole stored row**, hashed on every `MergeInPlace` integrate and re-touched by the cross-product probe. The optimizer has no column-liveness-through-join rule, so the full source row is stored even when only a few columns are live above the join (q4: bid needs `{auction,price,date_time}` of 7, auction `{id,category,date_time,expires}` of 10). `wide` stores the full row; `narrow` stores a 2-column projection — the prize a projection-pushdown rule (or columnar SoA) would capture.

Stream: 4,000 ticks × 256 rows/tick over ~32 join keys, median of 7 runs. Same delta/probe/key distribution; the ONLY difference is the stored inner-row width.

| Stored row | wide ns | wide B | narrow ns | narrow B | term-2 prize (ns) | prize % |
|:--|--:|--:|--:|--:|--:|--:|
| W8 (8 long) → W2 | 134.0 | 827 | 80.9 | 425 | 53.1 | 40% |
| WStr (3 long+str) → W2 | 178.3 | 559 | 75.7 | 425 | 102.6 | 58% |

