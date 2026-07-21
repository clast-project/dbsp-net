# DbspNet — ApplyOp alloc split (row-wise vs columnar)

## ApplyOp alloc split: row-wise vs columnar (§1 / §2.3)

**Reconstruction** of the uncommitted `ApplyOpAllocSplit` from `docs/design-columnar-batch1.md` §1/§2.3, grounded on the real `StructuralRow` + projection-ApplyOp path. One hot projection: a 14-col SCD-ish input row (8 boxed `long` + 6 `string`, pre-boxed) → 13 output cols (12 passthrough by reference + 1 computed `long`, the only new box/row). Absolute B/row is host-specific; the portable claim is the **relative** P→COL reduction.

Stream: 20,000 ticks × 1024 rows/tick, median of 5 runs. `B/row` is managed bytes per output row (`GC.GetAllocatedBytesForCurrentThread`). Host: .NET 10.0.1, 10 cores, Server GC.

| variant | ns/row | B/row |
|:--|--:|--:|
| P·fresh (row-wise, current) | 144.4 | 214.3 |
| P·pooled (pooled output dict) | 143.0 | 184.0 |
| P·noWrap (object[] only, no wrapper/hash) | 35.1 | 152.0 |
| **COL (columnar SoA, object arrays)** | 51.2 | **136.5** |

**P·fresh → COL: −36.3%** (doc ceiling: −38.3%).

Apportionment of P·fresh's per-row alloc:

| term | B/row | % of P |
|:--|--:|--:|
| (a) output container + dict entries | 30.3 | 14.1% |
| (b) StructuralRow wrapper + hash | 32.0 | 14.9% |
| (b) per-row object[] header/body | 15.5 | 7.3% |
| (c) boxed compute + amortised columns (COL floor) | 136.5 | 63.7% |

