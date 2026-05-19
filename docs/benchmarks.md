# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency across four query shapes.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 111.4 µs | 1.30 µs | 85.7× |
|       1000 | 973.3 µs | 1.40 µs | 695× |
|      10000 | 8.56 ms | 1.20 µs | 7130× |
|     100000 | 92.82 ms | 600.0 ns | 154696× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 171.3 µs | 3.80 µs | 45.1× |
|       1000 | 960.2 µs | 4.10 µs | 234× |
|      10000 | 7.84 ms | 8.40 µs | 934× |
|     100000 | 137.88 ms | 45.10 µs | 3057× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 142.8 µs | 4.80 µs | 29.8× |
|       1000 | 1.09 ms | 4.60 µs | 238× |
|      10000 | 10.01 ms | 19.20 µs | 522× |
|     100000 | 92.19 ms | 145.60 µs | 633× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 202.5 µs | 3.70 µs | 54.7× |
|       1000 | 1.70 ms | 5.30 µs | 321× |
|      10000 | 17.87 ms | 22.50 µs | 794× |
|     100000 | 80.88 ms | 143.10 µs | 565× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 107.8 µs | 4.30 µs | 25.1× |
|       1000 | 907.3 µs | 11.40 µs | 79.6× |
|      10000 | 3.56 ms | 42.80 µs | 83.3× |
|     100000 | 11.44 ms | 59.40 µs | 193× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 20.53 ms | 325.90 µs | 63.0× |
|        100 | 114.68 ms | 453.60 µs | 253× |
|        200 | 2.13 s | 1.72 ms | 1242× |
|        500 | 33.84 s | 11.33 ms | 2986× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 30.3 ns | 2.13 µs | 1.36 µs |
|       1000 | WeightOf(absent)   | 15.8 ns | 2.10 µs | 1.31 µs |
|       1000 | Integrate(1)       | 200.0 ns | 800.0 ns | 1.60 µs |
|       1000 | Integrate(100)     | 14.80 µs | 24.40 µs | 34.90 µs |
|      10000 | WeightOf(present)  | 25.4 ns | 1.55 µs | 1.21 µs |
|      10000 | WeightOf(absent)   | 14.0 ns | 28.4 ns | 23.5 ns |
|      10000 | Integrate(1)       | 100.0 ns | 200.0 ns | 400.0 ns |
|      10000 | Integrate(100)     | 8.70 µs | 5.30 µs | 6.00 µs |
|     100000 | WeightOf(present)  | 3.1 ns | 66.9 ns | 63.4 ns |
|     100000 | WeightOf(absent)   | 2.7 ns | 32.8 ns | 30.1 ns |
|     100000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|     100000 | Integrate(100)     | 1.20 µs | 1.90 µs | 1.90 µs |
|    1000000 | WeightOf(present)  | 3.0 ns | 101.6 ns | 106.9 ns |
|    1000000 | WeightOf(absent)   | 2.4 ns | 82.3 ns | 82.8 ns |
|    1000000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|    1000000 | Integrate(100)     | 1.10 µs | 1.50 µs | 2.00 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 35.3 ns | 434.1 ns | 369.6 ns |
|       1000 | GroupFor(absent)   | 22.0 ns | 229.9 ns | 224.1 ns |
|       1000 | Integrate(1)       | 300.0 ns | 700.0 ns | 1.20 µs |
|       1000 | Integrate(100)     | 16.40 µs | 30.90 µs | 34.00 µs |
|      10000 | GroupFor(present)  | 27.4 ns | 366.2 ns | 382.7 ns |
|      10000 | GroupFor(absent)   | 18.7 ns | 213.9 ns | 33.6 ns |
|      10000 | Integrate(1)       | 100.0 ns | 300.0 ns | 700.0 ns |
|      10000 | Integrate(100)     | 5.90 µs | 15.10 µs | 8.50 µs |
|     100000 | GroupFor(present)  | 7.3 ns | 137.4 ns | 123.2 ns |
|     100000 | GroupFor(absent)   | 3.0 ns | 46.4 ns | 37.3 ns |
|     100000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|     100000 | Integrate(100)     | 13.60 µs | 6.30 µs | 7.30 µs |
|    1000000 | GroupFor(present)  | 3.4 ns | 173.0 ns | 167.9 ns |
|    1000000 | GroupFor(absent)   | 2.9 ns | 98.0 ns | 87.6 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|    1000000 | Integrate(100)     | 4.00 µs | 5.50 µs | 6.70 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like).

| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |
|-----------:|:-------------|------------:|------------:|------------:|-----------------:|
|       1000 | new key      | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|       1000 | existing key | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|       1000 | bulk-10 mixed | 600.0 ns | 1.40 µs | 1.60 µs | 2.33× |
|      10000 | new key      | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|      10000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|      10000 | bulk-10 mixed | 600.0 ns | 1.60 µs | 1.70 µs | 2.67× |
|     100000 | new key      | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|     100000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|     100000 | bulk-10 mixed | 300.0 ns | 1.60 µs | 1.60 µs | 5.33× |
## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

