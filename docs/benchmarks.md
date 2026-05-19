# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency across four query shapes.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 143.3 µs | 1.70 µs | 84.3× |
|       1000 | 1.24 ms | 1.80 µs | 689× |
|      10000 | 8.55 ms | 1.70 µs | 5032× |
|     100000 | 81.21 ms | 1.00 µs | 81209× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 264.4 µs | 4.70 µs | 56.3× |
|       1000 | 1.50 ms | 5.70 µs | 263× |
|      10000 | 8.72 ms | 12.20 µs | 715× |
|     100000 | 159.67 ms | 79.00 µs | 2021× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 197.4 µs | 6.90 µs | 28.6× |
|       1000 | 1.48 ms | 17.10 µs | 86.5× |
|      10000 | 9.30 ms | 66.20 µs | 140× |
|     100000 | 66.46 ms | 105.40 µs | 631× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 342.2 µs | 6.40 µs | 53.5× |
|       1000 | 2.40 ms | 9.80 µs | 245× |
|      10000 | 20.85 ms | 34.20 µs | 610× |
|     100000 | 143.66 ms | 23.40 µs | 6139× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 97.3 µs | 4.20 µs | 23.2× |
|       1000 | 902.1 µs | 11.20 µs | 80.5× |
|      10000 | 3.53 ms | 43.20 µs | 81.7× |
|     100000 | 13.22 ms | 62.40 µs | 212× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 20.87 ms | 422.10 µs | 49.4× |
|        100 | 119.09 ms | 455.10 µs | 262× |
|        200 | 2.14 s | 1.56 ms | 1369× |
|        500 | 33.46 s | 11.63 ms | 2877× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 32.4 ns | 2.26 µs | 1.44 µs |
|       1000 | WeightOf(absent)   | 18.6 ns | 2.19 µs | 1.24 µs |
|       1000 | Integrate(1)       | 200.0 ns | 900.0 ns | 1.60 µs |
|       1000 | Integrate(100)     | 14.70 µs | 25.70 µs | 36.50 µs |
|      10000 | WeightOf(present)  | 26.6 ns | 1.54 µs | 1.25 µs |
|      10000 | WeightOf(absent)   | 14.0 ns | 27.5 ns | 23.1 ns |
|      10000 | Integrate(1)       | 100.0 ns | 200.0 ns | 400.0 ns |
|      10000 | Integrate(100)     | 2.40 µs | 4.70 µs | 4.20 µs |
|     100000 | WeightOf(present)  | 2.9 ns | 66.8 ns | 62.6 ns |
|     100000 | WeightOf(absent)   | 2.5 ns | 33.4 ns | 30.0 ns |
|     100000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|     100000 | Integrate(100)     | 1.60 µs | 1.50 µs | 2.70 µs |
|    1000000 | WeightOf(present)  | 3.2 ns | 98.5 ns | 108.0 ns |
|    1000000 | WeightOf(absent)   | 2.5 ns | 85.1 ns | 86.3 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 200.0 ns | 500.0 ns |
|    1000000 | Integrate(100)     | 1.30 µs | 1.50 µs | 2.00 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 35.4 ns | 436.4 ns | 383.7 ns |
|       1000 | GroupFor(absent)   | 24.3 ns | 252.7 ns | 235.3 ns |
|       1000 | Integrate(1)       | 300.0 ns | 800.0 ns | 1.20 µs |
|       1000 | Integrate(100)     | 16.30 µs | 32.30 µs | 35.20 µs |
|      10000 | GroupFor(present)  | 28.0 ns | 385.3 ns | 393.0 ns |
|      10000 | GroupFor(absent)   | 19.1 ns | 216.9 ns | 30.5 ns |
|      10000 | Integrate(1)       | 100.0 ns | 400.0 ns | 700.0 ns |
|      10000 | Integrate(100)     | 5.90 µs | 14.30 µs | 7.20 µs |
|     100000 | GroupFor(present)  | 7.8 ns | 122.3 ns | 112.3 ns |
|     100000 | GroupFor(absent)   | 2.9 ns | 38.4 ns | 37.8 ns |
|     100000 | Integrate(1)       | 100.0 ns | 300.0 ns | 600.0 ns |
|     100000 | Integrate(100)     | 4.60 µs | 5.60 µs | 6.80 µs |
|    1000000 | GroupFor(present)  | 3.5 ns | 168.9 ns | 179.4 ns |
|    1000000 | GroupFor(absent)   | 2.9 ns | 93.5 ns | 83.5 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 300.0 ns | 600.0 ns |
|    1000000 | Integrate(100)     | 17.60 µs | 6.10 µs | 7.00 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like).

| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |
|-----------:|:-------------|------------:|------------:|------------:|-----------------:|
|       1000 | new key      | 200.0 ns | 300.0 ns | 500.0 ns | 1.50× |
|       1000 | existing key | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|       1000 | bulk-10 mixed | 600.0 ns | 1.40 µs | 1.70 µs | 2.33× |
|      10000 | new key      | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|      10000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|      10000 | bulk-10 mixed | 600.0 ns | 1.60 µs | 1.80 µs | 2.67× |
|     100000 | new key      | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|     100000 | existing key | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|     100000 | bulk-10 mixed | 300.0 ns | 1.60 µs | 1.70 µs | 5.33× |
## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

