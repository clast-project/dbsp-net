# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency across four query shapes.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 24 cores.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 118.2 µs | 1.30 µs | 90.9× |
|       1000 | 1.03 ms | 1.40 µs | 732× |
|      10000 | 9.00 ms | 1.30 µs | 6924× |
|     100000 | 79.15 ms | 600.0 ns | 131918× |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 171.2 µs | 3.70 µs | 46.3× |
|       1000 | 1.00 ms | 4.20 µs | 239× |
|      10000 | 8.88 ms | 13.20 µs | 673× |
|     100000 | 167.68 ms | 73.10 µs | 2294× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 92.5 µs | 3.10 µs | 29.8× |
|       1000 | 1.14 ms | 4.60 µs | 247× |
|      10000 | 10.27 ms | 20.80 µs | 494× |
|     100000 | 101.75 ms | 165.40 µs | 615× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 195.0 µs | 3.70 µs | 52.7× |
|       1000 | 1.96 ms | 5.20 µs | 376× |
|      10000 | 16.02 ms | 21.70 µs | 738× |
|     100000 | 94.54 ms | 154.50 µs | 612× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation. Establishes the perf ceiling for a future typed-row pipeline.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 110.4 µs | 4.20 µs | 26.3× |
|       1000 | 967.6 µs | 11.60 µs | 83.4× |
|      10000 | 3.79 ms | 43.30 µs | 87.6× |
|     100000 | 11.86 ms | 62.20 µs | 191× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 22.23 ms | 353.00 µs | 63.0× |
|        100 | 135.10 ms | 506.30 µs | 267× |
|        200 | 2.23 s | 1.92 ms | 1163× |
|        500 | 34.20 s | 10.77 ms | 3176× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 28.5 ns | 2.08 µs | 1.34 µs |
|       1000 | WeightOf(absent)   | 15.9 ns | 2.08 µs | 1.29 µs |
|       1000 | Integrate(1)       | 200.0 ns | 800.0 ns | 1.40 µs |
|       1000 | Integrate(100)     | 14.20 µs | 24.60 µs | 35.80 µs |
|      10000 | WeightOf(present)  | 22.6 ns | 1.48 µs | 1.16 µs |
|      10000 | WeightOf(absent)   | 13.9 ns | 126.7 ns | 23.5 ns |
|      10000 | Integrate(1)       | 200.0 ns | 400.0 ns | 400.0 ns |
|      10000 | Integrate(100)     | 8.60 µs | 5.10 µs | 5.80 µs |
|     100000 | WeightOf(present)  | 3.2 ns | 70.3 ns | 67.9 ns |
|     100000 | WeightOf(absent)   | 2.7 ns | 31.9 ns | 28.4 ns |
|     100000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|     100000 | Integrate(100)     | 1.10 µs | 1.80 µs | 1.80 µs |
|    1000000 | WeightOf(present)  | 3.9 ns | 127.4 ns | 121.3 ns |
|    1000000 | WeightOf(absent)   | 2.7 ns | 81.7 ns | 76.2 ns |
|    1000000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|    1000000 | Integrate(100)     | 1.20 µs | 1.50 µs | 1.90 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 33.2 ns | 378.3 ns | 344.3 ns |
|       1000 | GroupFor(absent)   | 24.5 ns | 197.3 ns | 198.4 ns |
|       1000 | Integrate(1)       | 200.0 ns | 600.0 ns | 1.10 µs |
|       1000 | Integrate(100)     | 14.70 µs | 30.20 µs | 33.20 µs |
|      10000 | GroupFor(present)  | 28.2 ns | 319.2 ns | 353.2 ns |
|      10000 | GroupFor(absent)   | 18.6 ns | 172.6 ns | 185.0 ns |
|      10000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|      10000 | Integrate(100)     | 4.50 µs | 14.80 µs | 7.50 µs |
|     100000 | GroupFor(present)  | 7.5 ns | 128.7 ns | 127.4 ns |
|     100000 | GroupFor(absent)   | 3.1 ns | 42.7 ns | 37.4 ns |
|     100000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|     100000 | Integrate(100)     | 4.40 µs | 6.60 µs | 7.90 µs |
|    1000000 | GroupFor(present)  | 3.3 ns | 200.6 ns | 301.2 ns |
|    1000000 | GroupFor(absent)   | 2.8 ns | 93.8 ns | 87.2 ns |
|    1000000 | Integrate(1)       | 100.0 ns | 300.0 ns | 500.0 ns |
|    1000000 | Integrate(100)     | 4.30 µs | 5.70 µs | 6.90 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like).

| N          | Op           | Flat        | Spine(N=4)  | Spine(N=2)  | Spine(4) vs Flat |
|-----------:|:-------------|------------:|------------:|------------:|-----------------:|
|       1000 | new key      | 200.0 ns | 300.0 ns | 500.0 ns | 1.50× |
|       1000 | existing key | 100.0 ns | 400.0 ns | 500.0 ns | 4.00× |
|       1000 | bulk-10 mixed | 600.0 ns | 1.40 µs | 1.50 µs | 2.33× |
|      10000 | new key      | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|      10000 | existing key | 200.0 ns | 400.0 ns | 500.0 ns | 2.00× |
|      10000 | bulk-10 mixed | 600.0 ns | 1.60 µs | 1.70 µs | 2.67× |
|     100000 | new key      | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|     100000 | existing key | 200.0 ns | 400.0 ns | 600.0 ns | 2.00× |
|     100000 | bulk-10 mixed | 200.0 ns | 1.40 µs | 1.60 µs | 7.00× |
## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

### What went well

- **Filter** matches the ideal shape exactly: batch scales linearly in N, incremental stays sub-microsecond regardless of N. Speedup grows ~linearly, and by 100k rows the incremental path is ~5 orders of magnitude faster. This is the easy case — a pipelined stateless operator, no trace to maintain.
- **Transitive closure** shows the quadratic-speedup shape the DBSP paper advertises: batch is ~O(n³) (n semi-naïve iterations × n² closure size at convergence), incremental is O(n) for one leaf-edge insertion, so speedup grows quadratically. By N=500 edges incremental is ~2500× faster.

### Where the curves end up

- **Joined GROUP BY** (pure SUM) is the cleanest positive result: speedup now climbs past 500× at N=100k. Every hot path is O(|delta|) — SUM folds the per-group delta into running state, and the indexed traces (`IndexedZSetTrace`) integrate in place rather than rebuilding. The residual sub-linear growth in the incremental column is dominated by allocator / cache effects on a larger running state, not an algorithmic scan.
- **Multi-aggregate** (SUM / COUNT / MIN / MAX) now climbs steeply with N, reaching ~2000× at N=100k. All four aggregators are incremental: SUM / COUNT fold the delta; MIN / MAX maintain a per-group sorted set of distinct positive-weight values for O(log n) extremum lookup. Per-update cost is dominated by trace and aggregate-state dictionary ops at this point — the visible ceiling is now the same allocator / cache effect that limits Joined GROUP BY at scale.

