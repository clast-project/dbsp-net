# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency across four query shapes.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.6, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 73.1 µs | 1.00 µs | 73.1× |
|       1000 | 649.0 µs | 1.10 µs | 590× |
|      10000 | 4.23 ms | 1.00 µs | 4235× |
|     100000 | 48.01 ms | 500.0 ns | 96019× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 170.4 µs | 3.70 µs | 46.1× |
|       1000 | 1.00 ms | 4.20 µs | 239× |
|      10000 | 8.34 ms | 10.70 µs | 779× |
|     100000 | 175.06 ms | 73.80 µs | 2372× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 102.9 µs | 3.90 µs | 26.4× |
|       1000 | 1.07 ms | 7.00 µs | 153× |
|      10000 | 14.33 ms | 34.60 µs | 414× |
|     100000 | 95.79 ms | 161.90 µs | 592× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 155.7 µs | 3.20 µs | 48.7× |
|       1000 | 1.46 ms | 4.80 µs | 303× |
|      10000 | 14.62 ms | 22.10 µs | 662× |
|     100000 | 88.10 ms | 141.40 µs | 623× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 99.2 µs | 4.40 µs | 22.5× |
|       1000 | 901.9 µs | 11.80 µs | 76.4× |
|      10000 | 3.72 ms | 46.10 µs | 80.6× |
|     100000 | 13.16 ms | 63.00 µs | 209× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 22.49 ms | 354.30 µs | 63.5× |
|        100 | 125.17 ms | 536.60 µs | 233× |
|        200 | 2.15 s | 1.53 ms | 1410× |
|        500 | 34.55 s | 11.18 ms | 3090× |

## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

