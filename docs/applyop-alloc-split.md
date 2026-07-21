# DbspNet — ApplyOp alloc split (row-wise vs columnar)

## ApplyOp alloc split: row-wise → object-col → typed-col (§1 / §2.3 / §7 #3)

**Reconstruction** of the uncommitted `ApplyOpAllocSplit` from `docs/design-columnar-batch1.md`, grounded on the real `StructuralRow` + projection-ApplyOp path. Input row is 8 boxed `long` + 6 `string` (pre-boxed once; passthrough copies references). Absolute B/row is host-specific; the portable claim is the **relative** reductions.

Stream: 20,000 ticks × 256 rows/tick, median of 5 runs. `B/row` is managed bytes per output row (`GC.GetAllocatedBytesForCurrentThread`). Host: .NET 10.0.1, 10 cores, Server GC.

### Apportionment of P·fresh — mixed (8+4 pass, 1 computed)

| term | B/row | % of P |
|:--|--:|--:|
| (a) output container + dict entries | 32.6 | 15.0% |
| (b) StructuralRow wrapper + hash | 32.0 | 14.8% |
| (b) per-row object[] header/body | 14.2 | 6.6% |
| (c) boxed compute + amortised columns (COL floor) | 137.8 | 63.6% |

### Ladder: P·fresh → COL (object) → TCOL (typed), by projection shape

`COL↓` = COL reduction vs P·fresh (the §2.3 object-columnar ceiling). `TCOL↓` = TCOL reduction vs P·fresh. `typed↓` = TCOL's **marginal** gain over COL (the §7 #3 typed-column upside = boxing of freshly-computed numerics).

| scenario | fresh boxes/row | P·fresh B | COL B | TCOL B | COL↓ | TCOL↓ | typed↓ vs COL |
|:--|--:|--:|--:|--:|--:|--:|--:|
| mixed (8+4 pass, 1 computed) | 1 | 216.6 | 137.8 | 113.9 | −36.4% | −47.4% | −17.3% |
| numeric-heavy (2 pass, 10 computed) | 10 | 432.6 | 353.8 | 113.9 | −18.2% | −73.7% | −67.8% |

Reading: `long[]` and `object?[]` are both 8 B/element, so TCOL never shrinks column storage — `typed↓ vs COL` is purely the eliminated boxes of computed numerics. It is small when the projection mostly passes values through (mixed) and grows with computed-numeric width (numeric-heavy), confirming §2.3/§23: object-columnar captures the bulk; typed-columnar's extra upside is the residual boxing term, worth its larger scope only where operators newly produce many numeric columns.

