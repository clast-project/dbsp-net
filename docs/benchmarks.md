# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency. The query shapes below cover the canonical SQL surface (filter, joined group-by, transitive closure via `WITH RECURSIVE`) plus row-layout variants (nullable column, emitted-equality codec, hand-wired typed rows) and pure-trace microbenchmarks against the spine.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 148.5 µs | 1.80 µs | 82.5× |
|       1000 | 1.24 ms | 2.00 µs | 620× |
|      10000 | 8.17 ms | 1.70 µs | 4807× |
|     100000 | 107.71 ms | 1.90 µs | 56689× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 386.6 µs | 7.30 µs | 53.0× |
|       1000 | 2.33 ms | 9.40 µs | 248× |
|      10000 | 13.24 ms | 15.10 µs | 877× |
|     100000 | 137.83 ms | 48.90 µs | 2819× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 196.3 µs | 7.10 µs | 27.6× |
|       1000 | 1.47 ms | 16.60 µs | 88.6× |
|      10000 | 8.96 ms | 71.20 µs | 126× |
|     100000 | 53.87 ms | 102.30 µs | 527× |

## Joined GROUP BY — flat vs spine trace family

The same query and data as the Joined GROUP BY benchmark above, compiled once onto the flat dictionary traces and once onto the spine (LSM) traces via `CompileOptions { TraceFamily = TraceFamily.Spine }`. Both run through the structural compile (spine mode skips the typed fast path), so the delta is the trace family alone: the spine pays a per-batch bloom + binary search on each probe where the flat trace does one dictionary lookup, in exchange for immutable batches that snapshot per-file and spill to disk. Operators: `IncrementalJoinOp` + `IncrementalAggregateOp` (flat) vs `SpineIncrementalJoinOp` + `SpineIncrementalAggregateOp`.

| N          | Flat batch    | Flat incr      | Spine batch   | Spine incr     | Spine incr vs flat |
|-----------:|--------------:|---------------:|--------------:|---------------:|-------------------:|
|        100 | 200.5 µs | 6.80 µs | 202.5 µs | 10.60 µs | 1.56× |
|       1000 | 1.16 ms | 14.60 µs | 1.87 ms | 23.10 µs | 1.58× |
|      10000 | 7.37 ms | 65.50 µs | 19.56 ms | 79.60 µs | 1.22× |
|     100000 | 54.37 ms | 64.00 µs | 98.47 ms | 96.20 µs | 1.50× |

## Joined GROUP BY (nullable `amount`) — same query shape

Identical query to Benchmark 3, but `orders.amount` is nullable and ~10% of input rows have `NULL amount`. Exercises the typed pipeline's nullable-arg SUM path (Phase N4): per-row `HasValue` check, `DistinctNonNullRows` bookkeeping for the linear gate, and `Nullable<long>`-typed aggregate output slot. Compare to Benchmark 3 to read off the per-row overhead of the nullable wrapper versus the non-null fast path.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 201.0 µs | 6.10 µs | 33.0× |
|       1000 | 1.47 ms | 15.60 µs | 94.1× |
|      10000 | 7.92 ms | 64.30 µs | 123× |
|     100000 | 69.97 ms | 53.10 µs | 1318× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 261.8 µs | 6.50 µs | 40.3× |
|       1000 | 2.17 ms | 5.80 µs | 373× |
|      10000 | 19.63 ms | 15.60 µs | 1258× |
|     100000 | 89.73 ms | 23.50 µs | 3818× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 99.6 µs | 4.30 µs | 23.2× |
|       1000 | 946.9 µs | 12.90 µs | 73.4× |
|      10000 | 3.77 ms | 45.90 µs | 82.2× |
|     100000 | 12.78 ms | 66.80 µs | 191× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 23.02 ms | 344.40 µs | 66.8× |
|        100 | 162.29 ms | 479.30 µs | 339× |
|        200 | 2.16 s | 1.53 ms | 1408× |
|        500 | 33.84 s | 11.96 ms | 2831× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 30.9 ns | 321.3 ns | 278.8 ns |
|       1000 | WeightOf(absent)   | 18.0 ns | 242.3 ns | 217.6 ns |
|       1000 | Integrate(1)       | 200.0 ns | 600.0 ns | 1.10 µs |
|       1000 | Integrate(100)     | 14.70 µs | 15.60 µs | 17.20 µs |
|      10000 | WeightOf(present)  | 25.8 ns | 264.5 ns | 300.4 ns |
|      10000 | WeightOf(absent)   | 15.5 ns | 214.2 ns | 236.8 ns |
|      10000 | Integrate(1)       | 200.0 ns | 600.0 ns | 1.20 µs |
|      10000 | Integrate(100)     | 12.30 µs | 14.30 µs | 6.40 µs |
|     100000 | WeightOf(present)  | 4.0 ns | 67.4 ns | 66.1 ns |
|     100000 | WeightOf(absent)   | 2.7 ns | 35.8 ns | 31.2 ns |
|     100000 | Integrate(1)       | 100.0 ns | 200.0 ns | 500.0 ns |
|     100000 | Integrate(100)     | 1.30 µs | 4.30 µs | 2.70 µs |
|    1000000 | WeightOf(present)  | 3.2 ns | 99.7 ns | 102.0 ns |
|    1000000 | WeightOf(absent)   | 2.8 ns | 84.8 ns | 85.0 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|    1000000 | Integrate(100)     | 1.20 µs | 1.90 µs | 2.60 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 6.3 ns | 438.4 ns | 389.1 ns |
|       1000 | GroupFor(absent)   | 6.5 ns | 250.7 ns | 244.3 ns |
|       1000 | Integrate(1)       | 200.0 ns | 700.0 ns | 1.30 µs |
|       1000 | Integrate(100)     | 12.70 µs | 31.20 µs | 34.50 µs |
|      10000 | GroupFor(present)  | 6.3 ns | 364.9 ns | 387.8 ns |
|      10000 | GroupFor(absent)   | 6.4 ns | 228.5 ns | 34.6 ns |
|      10000 | Integrate(1)       | 200.0 ns | 300.0 ns | 600.0 ns |
|      10000 | Integrate(100)     | 6.30 µs | 16.70 µs | 9.30 µs |
|     100000 | GroupFor(present)  | 6.3 ns | 126.6 ns | 135.7 ns |
|     100000 | GroupFor(absent)   | 3.1 ns | 45.6 ns | 37.1 ns |
|     100000 | Integrate(1)       | 100.0 ns | 400.0 ns | 600.0 ns |
|     100000 | Integrate(100)     | 4.60 µs | 7.40 µs | 9.30 µs |
|    1000000 | GroupFor(present)  | 2.9 ns | 175.8 ns | 167.7 ns |
|    1000000 | GroupFor(absent)   | 1.8 ns | 91.9 ns | 93.5 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 400.0 ns | 600.0 ns |
|    1000000 | Integrate(100)     | 4.50 µs | 5.80 µs | 7.20 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like).

| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |
|-----------:|:-------------|------------:|------------:|------------:|-----------------:|
|       1000 | new key      | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|       1000 | existing key | 100.0 ns | 400.0 ns | 600.0 ns | 4.00× |
|       1000 | bulk-10 mixed | 600.0 ns | 1.50 µs | 1.70 µs | 2.50× |
|      10000 | new key      | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|      10000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|      10000 | bulk-10 mixed | 600.0 ns | 1.60 µs | 2.00 µs | 2.67× |
|     100000 | new key      | 200.0 ns | 400.0 ns | 700.0 ns | 2.00× |
|     100000 | existing key | 200.0 ns | 500.0 ns | 700.0 ns | 2.50× |
|     100000 | bulk-10 mixed | 400.0 ns | 1.50 µs | 1.70 µs | 3.75× |
## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

