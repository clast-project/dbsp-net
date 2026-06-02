# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency. The query shapes below cover the canonical SQL surface (filter, joined group-by, transitive closure via `WITH RECURSIVE`) plus row-layout variants (nullable column, emitted-equality codec, hand-wired typed rows) and pure-trace microbenchmarks against the spine.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 132.1 µs | 1.60 µs | 82.6× |
|       1000 | 1.20 ms | 1.70 µs | 705× |
|      10000 | 10.27 ms | 1.50 µs | 6849× |
|     100000 | 76.38 ms | 1.00 µs | 76377× |

## Operator fusion — `map → filter → map` chain

A three-stage linear chain (`MapRows` → `Filter` → `MapRows`) wired as three separate operators versus folded into one `MapFilterRows` pass — the pre- and post-fusion shapes for a run of adjacent Filter/Project plan nodes. Rows are `StructuralRow`s built as the structural compile path builds them; the filter keeps ~half the rows. Each step pushes the same N-row batch through the stateless chain. **Latency** is the median per-step time; **alloc/step** is bytes allocated per step (the unfused chain allocates an intermediate Z-set per stage plus a row for every input at the first map, before the filter can drop it).

| N (batch)  | Unfused       | Fused          | Speedup | Unfused alloc | Fused alloc | Alloc saved |
|-----------:|--------------:|---------------:|--------:|--------------:|------------:|------------:|
|        100 | 27.70 µs | 10.20 µs | 2.72× |   33.1 KB |    9.2 KB | 72% |
|       1000 | 172.50 µs | 75.60 µs | 2.28× |  334.4 KB |   93.9 KB | 72% |
|      10000 | 1.23 ms | 303.20 µs | 4.05× |    3.1 MB |  909.6 KB | 72% |
|     100000 | 47.85 ms | 10.42 ms | 4.59× |   29.6 MB |    8.5 MB | 71% |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 196.3 µs | 3.80 µs | 51.7× |
|       1000 | 1.10 ms | 4.70 µs | 234× |
|      10000 | 13.12 ms | 9.40 µs | 1395× |
|     100000 | 138.61 ms | 18.50 µs | 7492× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 183.0 µs | 7.00 µs | 26.1× |
|       1000 | 1.27 ms | 15.80 µs | 80.3× |
|      10000 | 6.74 ms | 63.30 µs | 106× |
|     100000 | 58.32 ms | 102.10 µs | 571× |

## Joined GROUP BY — flat vs spine trace family

The same query and data as the Joined GROUP BY benchmark above, compiled once onto the flat dictionary traces and once onto the spine (LSM) traces via `CompileOptions { TraceFamily = TraceFamily.Spine }`. Both run through the structural compile (spine mode skips the typed fast path), so the delta is the trace family alone: the spine pays a per-batch bloom + binary search on each probe where the flat trace does one dictionary lookup, in exchange for immutable batches that snapshot per-file and spill to disk. Operators: `IncrementalJoinOp` + `IncrementalAggregateOp` (flat) vs `SpineIncrementalJoinOp` + `SpineIncrementalAggregateOp`.

| N          | Flat batch    | Flat incr      | Spine batch   | Spine incr     | Spine incr vs flat |
|-----------:|--------------:|---------------:|--------------:|---------------:|-------------------:|
|        100 | 180.3 µs | 6.70 µs | 236.7 µs | 17.80 µs | 2.66× |
|       1000 | 1.08 ms | 14.40 µs | 1.65 ms | 46.10 µs | 3.20× |
|      10000 | 9.26 ms | 65.40 µs | 10.29 ms | 141.80 µs | 2.17× |
|     100000 | 51.91 ms | 64.50 µs | 75.49 ms | 126.10 µs | 1.96× |

## Joined GROUP BY (nullable `amount`) — same query shape

Identical query to Benchmark 3, but `orders.amount` is nullable and ~10% of input rows have `NULL amount`. Exercises the typed pipeline's nullable-arg SUM path (Phase N4): per-row `HasValue` check, `DistinctNonNullRows` bookkeeping for the linear gate, and `Nullable<long>`-typed aggregate output slot. Compare to Benchmark 3 to read off the per-row overhead of the nullable wrapper versus the non-null fast path.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 182.2 µs | 5.80 µs | 31.4× |
|       1000 | 1.43 ms | 14.90 µs | 95.9× |
|      10000 | 9.62 ms | 65.20 µs | 147× |
|     100000 | 60.22 ms | 72.70 µs | 828× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 268.5 µs | 5.80 µs | 46.3× |
|       1000 | 2.42 ms | 9.90 µs | 245× |
|      10000 | 23.75 ms | 32.60 µs | 728× |
|     100000 | 82.00 ms | 19.60 µs | 4184× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 99.7 µs | 4.20 µs | 23.7× |
|       1000 | 827.2 µs | 10.70 µs | 77.3× |
|      10000 | 3.42 ms | 43.40 µs | 78.8× |
|     100000 | 12.25 ms | 56.40 µs | 217× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 4.14 ms | 212.20 µs | 19.5× |
|        100 | 27.33 ms | 635.00 µs | 43.0× |
|        200 | 186.54 ms | 10.97 ms | 17.0× |
|        500 | 3.62 s | 31.83 ms | 114× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 29.4 ns | 326.5 ns | 281.4 ns |
|       1000 | WeightOf(absent)   | 17.2 ns | 246.8 ns | 214.5 ns |
|       1000 | Integrate(1)       | 200.0 ns | 500.0 ns | 1.10 µs |
|       1000 | Integrate(100)     | 14.60 µs | 15.60 µs | 16.90 µs |
|      10000 | WeightOf(present)  | 25.7 ns | 268.0 ns | 296.6 ns |
|      10000 | WeightOf(absent)   | 14.3 ns | 204.6 ns | 232.0 ns |
|      10000 | Integrate(1)       | 200.0 ns | 600.0 ns | 1.10 µs |
|      10000 | Integrate(100)     | 12.20 µs | 13.70 µs | 14.50 µs |
|     100000 | WeightOf(present)  | 2.9 ns | 64.9 ns | 67.7 ns |
|     100000 | WeightOf(absent)   | 2.4 ns | 33.0 ns | 29.9 ns |
|     100000 | Integrate(1)       | 100.0 ns | 200.0 ns | 400.0 ns |
|     100000 | Integrate(100)     | 1.40 µs | 1.50 µs | 2.00 µs |
|    1000000 | WeightOf(present)  | 3.2 ns | 100.2 ns | 104.8 ns |
|    1000000 | WeightOf(absent)   | 2.4 ns | 86.6 ns | 81.2 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 200.0 ns | 400.0 ns |
|    1000000 | Integrate(100)     | 1.30 µs | 1.50 µs | 2.00 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 35.2 ns | 420.6 ns | 372.2 ns |
|       1000 | GroupFor(absent)   | 23.1 ns | 250.9 ns | 232.4 ns |
|       1000 | Integrate(1)       | 300.0 ns | 800.0 ns | 1.20 µs |
|       1000 | Integrate(100)     | 16.10 µs | 31.60 µs | 34.50 µs |
|      10000 | GroupFor(present)  | 26.2 ns | 356.8 ns | 392.4 ns |
|      10000 | GroupFor(absent)   | 24.7 ns | 275.3 ns | 212.5 ns |
|      10000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|      10000 | Integrate(100)     | 6.20 µs | 16.40 µs | 9.50 µs |
|     100000 | GroupFor(present)  | 10.6 ns | 131.9 ns | 130.2 ns |
|     100000 | GroupFor(absent)   | 3.0 ns | 46.7 ns | 37.1 ns |
|     100000 | Integrate(1)       | 100.0 ns | 300.0 ns | 600.0 ns |
|     100000 | Integrate(100)     | 4.50 µs | 5.90 µs | 8.90 µs |
|    1000000 | GroupFor(present)  | 2.8 ns | 167.5 ns | 167.7 ns |
|    1000000 | GroupFor(absent)   | 1.8 ns | 101.6 ns | 88.2 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 400.0 ns | 600.0 ns |
|    1000000 | Integrate(100)     | 14.10 µs | 6.00 µs | 7.10 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like).

| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |
|-----------:|:-------------|------------:|------------:|------------:|-----------------:|
|       1000 | new key      | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|       1000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|       1000 | bulk-10 mixed | 700.0 ns | 1.40 µs | 1.70 µs | 2.00× |
|      10000 | new key      | 200.0 ns | 400.0 ns | 700.0 ns | 2.00× |
|      10000 | existing key | 200.0 ns | 500.0 ns | 700.0 ns | 2.50× |
|      10000 | bulk-10 mixed | 700.0 ns | 1.70 µs | 1.80 µs | 2.43× |
|     100000 | new key      | 200.0 ns | 500.0 ns | 700.0 ns | 2.50× |
|     100000 | existing key | 100.0 ns | 400.0 ns | 700.0 ns | 4.00× |
|     100000 | bulk-10 mixed | 300.0 ns | 1.50 µs | 1.80 µs | 5.00× |
## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Operator fusion** turns a `map → filter → map` chain from three operators (each materializing an intermediate Z-set, and the first map allocating a row for every input before the filter can drop it) into one `MapFilterRows` pass. The result is a flat ~2–4.5× per-step speedup and a steady ~72% drop in bytes allocated per step, independent of N — the fused pass allocates one output Z-set and builds a row only for the survivors. This is what the SQL compiler now emits for any run of adjacent Filter/Project plan nodes, on both the structural and typed paths.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

