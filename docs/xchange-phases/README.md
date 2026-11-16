# Where the TPC-DI program's exchanges spend their own work (ivm-bench batch 1, W=12), 2026-11-15

The question: does any one piece of the exchanges' own work (fused map, partition + split, gather,
or one site) earn a design? Decision rule: **< 5% of the batch-1 step wall and < 10% of allocation →
stop; ≥ 10% → design; in between → ask.**

## Setup
- Binary `b84c7ae`: `99ecf1c` plus a per-op exchange phase profile. Each exchange records
  `map / split / wait / key / add / build` (wall ticks, bytes allocated on the stepping thread, and
  the process GC pause that elapsed inside the phase). Every operator also records the GC pause it
  overlapped. Map (the fused pre-step) and key (the gather's key projection) are timed per row only
  under `DBSPNET_PROFILE_XPHASE=1`.
- `xphase.sh ARM NAME [DETAIL]` is `prof1.sh` without the view filter. Same server-like settings
  (incremental output, broadcast ≤ 1,000, size input order, `gcServer=1`, `gcConcurrent=0`,
  `GCDynamicAdaptationMode=0`), SF=3, `~/tpcdi-ab/runs/x*.prof`. `xloop.sh` ran them.
- 7 plain runs (`plain-w12-20261115.txt`) and 3 per-row detail runs (`detail-w12-20261115.txt`,
  `by-consumer-w12-20261115.txt`), from `summarize-exchange-phases.py` and `by-consumer.py`. All runs
  passed. Step median 13,177 ms (12,728–13,396), 62.63 GiB allocated, GC pause 6.5 s.
- "ms" below is summed over the 12 workers, ex-GC. "% step" = ms ÷ 12 ÷ step wall.

## 1. GC pause explains the census's hot sites
Under Server GC a blocking pause stops every worker in whatever operator it is running. The census
charged that time to the operator.

| op | view | census own work | GC pause inside (median) | own ex-GC (median of 7) | ns/row ex-GC |
|--:|---|--:|--:|--:|--:|
| 334 | `fact_holdings` | 4,132 ms | 4,819 ms (up to 9.8 s) | **718 ms** | 290 |
| 230 | `watches_history` | 2,273 ms | 0–1,400 ms | **723 ms** | 803 |

- **Op 334** consumes join 333's 2.48M-row, ~930 MiB output, and the young-gen budget runs out right
  there. Ex-GC it runs at 290 ns/row, which matches the §22.8 bench (~350 ns/row split + gather).
  It is the dim_account join's input exchange. Its fused map rewrites the wide joined row
  (351 ms, 568 MiB = 230 B/row).
- **Op 230**'s cost is the gather's inserts (356 ms, plus 227 key). It keys `watches_history` by
  `symbol` for the `securities` join. That gives ~3.6k keys, so each per-key `ZSetBuilder` holds
  hundreds of rows and every row pays a whole-row hash and an equality check. Unique-key sites such as
  172 (`fact_trade`) take 63 ms to insert 982k rows, because a single-entry group stores the row
  inline. The 2.5 µs/row the census saw was mostly GC and run-to-run spread (524–1,523 ms over 7 runs).
- The exchanges overlap **13.9 s of GC pause** (8.7% of step). That is the batch's GC landing where
  allocation runs out, not exchange work, and it is excluded everywhere below.

## 2. The decomposition
All 138 exchanges: **own work 16.9 s = 10.7% of step, 10.1 GiB = 15.7% of allocation** (wait
13.6 s = 8.6%; settled as the straggler bound, not re-examined).

| piece | ms ex-GC | % step | alloc MiB | % alloc | from |
|---|--:|--:|--:|--:|---|
| fused map (pre-step row rewrite) | 3,849 | 2.4% | 2,724 | 4.2% | detail |
| partition + bucket | 4,375 | 2.7% | 2,079 | 3.2% | detail |
| **split total (map + partition + bucket)** | 8,209 | **5.1%** | 4,804 | **7.5%** | plain |
| gather key projection (`ExchangeIndexStepOp` only) | 3,181 | 2.0% | 1,647 | 2.6% | detail |
| gather inserts | 4,967 | 3.1% | 3,365 | 5.2% | detail |
| gather final index build | 492 | 0.3% | 281 | 0.4% | detail |
| **gather total (key + inserts + build)** | 9,002 | **5.6%** | 5,293 | **8.2%** | plain |
| largest single site (235, `watches` aggregate input) | 2,218 | 1.4% | 778 | 1.2% | plain |

Per-row timing in the detail runs adds ~1–2% to own work. The plain-run totals are the clean ones.

By kind: `ExchangeIndexStepOp` (62) 7.3% of step / 11.4% of alloc, `ExchangeIndexOp` (11) 1.9% /
2.5%, `ExchangeStepOp` (1) 0.9% / 0.7%, `ExchangeOp` (50) 0.8% / 1.1%, broadcast 0.

By consumer (detail runs): join inputs (44 + 12 left joins) carry most of it: map 3.6 s, key 2.9 s,
inserts 2.6 s. The 12 aggregate inputs cost 1.3 s in partition + bucket (663 MiB, ~250 B/row at
site 235) and 1.2 s in inserts.

## 3. Findings that aren't one piece
- **An aggregate's partition builds the group-key row twice.** `PlanToCircuit.cs` partitions with
  `StablePartitionHash.OfWholeRow(KeyRowOf(row), …)`, which builds and drops an `object?[]` and a
  `StructuralRow` per row. Then the gather calls `KeyRowOf` again. This is ~1.3 s and 0.66 GiB at
  most (0.8% of step, 1% of alloc), the aggregate-input split row above.
- **`StablePartitionHash.OfRow` / `OfWholeRow`** allocate an `int[]` per row for multi-column keys,
  and `row[i]` boxes typed cells. `StableHash.Combine` is a fold, so an allocation-free form would
  place rows identically. This is part of the 2.7% partition + bucket line.
- **What Feldera does instead** (`crates/dbsp/src/operator/dynamic/communication/shard.rs:419`):
  the input is already an indexed, sorted batch. The sharder walks it by cursor, hashes the **key**
  once per key (not per row, never the whole row), and appends key and values in order to one builder
  per destination. The receiver `merge_batches` the W sorted batches. No per-row hash map, no rehash,
  no key re-projection. Adopting that shape would replace the 5.6% gather. But the consumers (joins,
  aggregates) read hash-indexed Z-sets, so it is the engine's representation choice
  (`docs/comparison-feldera-decisions.md`), not an exchange change.

## Decision: closed (Curt, 2026-11-15)
Split (5.1% / 7.5%) and gather (5.6% / 8.2%) both fell between the stop and go lines, so Curt made
the call and closed it. Neither piece can be removed whole. The index has to be built somewhere,
because joins and aggregates read hash-indexed Z-sets. Feldera's shard-and-merge shape belongs to the
engine-wide sorted-representation question. The cheap partition-hash cleanups (§3) top out around
1–2% and were not taken. Reopen only alongside a sorted-batch representation, or if a workload makes
one site, not the whole set, large.
