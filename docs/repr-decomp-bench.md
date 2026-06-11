# DbspNet — representation/execution decomposition

## Representation-vs-execution decomposition (§17)

Per-tick hot loop (build a delta dictionary of `D` wide-row keys, fold it into retained state, enumerate the changed entries) timed four ways at three key widths. `gen·fresh` is today's model. The successive deltas apportion the per-tuple floor:

- **alloc tax** = `gen·fresh − gen·pooled` (heap throughput: fresh `Dictionary`/tick).
- **abstraction tax** = `gen·pooled − mono·deleg` (ZSet/IZRing/Optional generic wrapper).
- **dispatch tax** = `mono·deleg − mono·inline` (per-row `Func<>` delegate call).
- **compute floor** = `mono·inline` (irreducible wide-key hash + arithmetic).

Stream: 8,000 ticks × 256 rows/tick, median of 5 runs. `ns/row` is per delta row; `B/row` is managed bytes per delta row (`GC.GetAllocatedBytesForCurrentThread`). Host: .NET 10.0.9, 24 cores, Server GC.

| Width | gen·fresh ns | gen·pooled ns | mono·deleg ns | mono·inline ns | alloc | abstraction | dispatch | floor |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| W2 (2 long) | 50.5 (308B) | 19.8 (0B) | 27.6 (0B) | 16.8 (0B) | 30.7 | -7.8 | 10.8 | 16.8 |
| W8 (8 long) | 128.3 (711B) | 58.8 (0B) | 59.5 (0B) | 43.6 (0B) | 69.6 | -0.7 | 15.9 | 43.6 |
| WStr (3 long+str) | 164.4 (442B) | 84.0 (0B) | 89.3 (0B) | 80.9 (0B) | 80.4 | -5.3 | 8.4 | 80.9 |

## Join-trace integrate: wide-stored vs narrow-stored inner multiset (§21)

The term-2 site §18 narrowing cannot reach: `IncrementalJoinOp`'s trace is an `IndexedZSet<joinKey, storedRow>` whose inner `ZSet` is keyed by the **whole stored row**, hashed on every `MergeInPlace` integrate and re-touched by the cross-product probe. The optimizer has no column-liveness-through-join rule, so the full source row is stored even when only a few columns are live above the join (q4: bid needs `{auction,price,date_time}` of 7, auction `{id,category,date_time,expires}` of 10). `wide` stores the full row; `narrow` stores a 2-column projection — the prize a projection-pushdown rule (or columnar SoA) would capture.

Stream: 8,000 ticks × 256 rows/tick over ~32 join keys, median of 5 runs. Same delta/probe/key distribution; the ONLY difference is the stored inner-row width.

| Stored row | wide ns | wide B | narrow ns | narrow B | term-2 prize (ns) | prize % |
|:--|--:|--:|--:|--:|--:|--:|
| W8 (8 long) → W2 | 128.9 | 827 | 70.5 | 425 | 58.4 | 45% |
| WStr (3 long+str) → W2 | 169.9 | 559 | 71.3 | 425 | 98.6 | 58% |

## Partitioned TOP-K: wide-stored vs narrow-stored ranking state (§22)

The q18/q19 site. `PartitionedTopKOp` keeps, per partition, a `SortedDictionary<wideRow,long>` (`_accum`) ordered by the ORDER BY key with a full-row tiebreak, plus a `Dictionary<wideRow,long>` window — both keyed by the **whole 7-column bid row** (incl. the `url`/`extra` strings), even though ranking needs only `{partition, order}`. `wide` stores the full row in that state; `narrow` stores `{order, rowRef}` and **materialises the full row from a pool only for the ≤k survivors** it outputs. Output is wide in BOTH (the fetch-back recovers it), so the prize is purely the cheaper in-state compare+hash — the in-`Step` op term the W>1 step decomposition attributes to the operator (not the exchange).

Stream: 3,906 ticks × 256 rows/tick (~1,000,000 append-only inserts), median of 5 runs. `narrow+fetch` is the real net (recovers wide survivors); `narrow-raw` emits the narrow row (no recovery) — the unreachable lower bound.

`wide·sorted` keeps the WIDE row but swaps the `Dictionary` window for a comparison-based `SortedDictionary` (no whole-row HASH; the prize §19's container swap left on the table — but a TOP-K container change is off-limits after §19's unexplained q9 regression). `wide → wide·sorted` = the *kill-the-hash* share; `wide·sorted → narrow+fetch` = the *shrink-the-key* residual the real row-narrowing rewrite must earn against its extractor + fetch-back cost.

| Shape | wide ns | wide·sorted ns | narrow+fetch ns | n+f B | total prize % | kill-hash % | shrink-key % | narrow-raw ns |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| q18 (TOP-1, tiny partitions) | 701.8 | 523.9 | 348.1 | 678 | 50% | 25% | 25% | 230.6 |
| q19 (TOP-10, accumulating) | 5080.1 | 3058.8 | 922.6 | 1458 | 82% | 40% | 42% | 856.5 |

