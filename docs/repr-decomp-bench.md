# DbspNet — representation/execution decomposition

## Representation-vs-execution decomposition (§17)

Per-tick hot loop (build a delta dictionary of `D` wide-row keys, fold it into retained state, enumerate the changed entries) timed four ways at three key widths. `gen·fresh` is today's model. The successive deltas apportion the per-tuple floor:

- **alloc tax** = `gen·fresh − gen·pooled` (heap throughput: fresh `Dictionary`/tick).
- **abstraction tax** = `gen·pooled − mono·deleg` (ZSet/IZRing/Optional generic wrapper).
- **dispatch tax** = `mono·deleg − mono·inline` (per-row `Func<>` delegate call).
- **compute floor** = `mono·inline` (irreducible wide-key hash + arithmetic).

Stream: 25,000 ticks × 256 rows/tick, median of 11 runs. `ns/row` is per delta row; `B/row` is managed bytes per delta row (`GC.GetAllocatedBytesForCurrentThread`). Host: .NET 10.0.9, 24 cores, Server GC.

| Width | gen·fresh ns | gen·pooled ns | mono·deleg ns | mono·inline ns | alloc | abstraction | dispatch | floor |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| W2 (2 long) | 50.5 (308B) | 19.7 (0B) | 27.2 (0B) | 16.9 (0B) | 30.8 | -7.4 | 10.3 | 16.9 |
| W8 (8 long) | 116.0 (711B) | 57.5 (0B) | 61.0 (0B) | 47.0 (0B) | 58.5 | -3.5 | 14.1 | 47.0 |
| WStr (3 long+str) | 168.3 (442B) | 84.5 (0B) | 98.1 (0B) | 80.1 (0B) | 83.7 | -13.6 | 18.0 | 80.1 |

