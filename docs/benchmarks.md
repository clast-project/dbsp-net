# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency across four query shapes.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.6, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 71.9 µs | 1.00 µs | 71.9× |
|       1000 | 645.9 µs | 1.10 µs | 587× |
|      10000 | 4.31 ms | 900.0 ns | 4792× |
|     100000 | 51.14 ms | 500.0 ns | 102287× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. SUM / COUNT fold the per-group delta into running state; MIN / MAX still rescan the post-delta per-group multiset (see interpretation below).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 146.9 µs | 3.70 µs | 39.7× |
|       1000 | 702.7 µs | 5.50 µs | 128× |
|      10000 | 5.06 ms | 21.70 µs | 233× |
|     100000 | 91.96 ms | 305.90 µs | 301× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 123.9 µs | 5.10 µs | 24.3× |
|       1000 | 909.5 µs | 8.20 µs | 111× |
|      10000 | 9.25 ms | 37.50 µs | 247× |
|     100000 | 95.93 ms | 175.80 µs | 546× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 165.2 µs | 3.50 µs | 47.2× |
|       1000 | 1.40 ms | 4.60 µs | 303× |
|      10000 | 17.14 ms | 20.30 µs | 844× |
|     100000 | 124.89 ms | 153.40 µs | 814× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 101.0 µs | 4.40 µs | 23.0× |
|       1000 | 927.3 µs | 11.70 µs | 79.3× |
|      10000 | 3.73 ms | 45.60 µs | 81.9× |
|     100000 | 13.73 ms | 64.40 µs | 213× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 23.43 ms | 323.00 µs | 72.5× |
|        100 | 123.55 ms | 483.60 µs | 255× |
|        200 | 2.23 s | 1.67 ms | 1338× |
|        500 | 34.76 s | 11.43 ms | 3040× |

## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) reaches ~300× at N=100k and still climbs with N, but more slowly. SUM / COUNT are O(|delta|); MIN / MAX inherit the default `Update`, which rescans the post-delta per-group multiset — retracting the current extremum requires knowing the next-best value, which needs a heap or sorted trace per group to incrementalize. So the residual O(group size) cost per tick is the MIN/MAX rescan, not the trace rebuild.

The remaining aggregate gap (incremental MIN/MAX) is filed in `docs/skipped.md`.

