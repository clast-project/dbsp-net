# DbspNet — benchmarks

Cold-batch vs. steady-state incremental latency. The query shapes below cover the canonical SQL surface (filter, joined group-by, transitive closure via `WITH RECURSIVE`) plus row-layout variants (nullable column, emitted-equality codec, hand-wired typed rows) and pure-trace microbenchmarks against the spine.
Each row: load the circuit fresh, measure the full compile-load-step time ("batch"); separately, load the circuit once, push one additional row and measure just `Step()` ("incremental"). Both are medians of multiple runs with a warmup pass.

Every measurement uses the plan optimizer (`PlanOptimizer.Optimize`).

Host: .NET 10.0.8, 14 cores, macOS 27.2.0 (Arm64). Generated 2026-11-15.

## Filter — `WHERE value > 500 AND status = 'active'`

Pipelined filter over a flat table. No stateful operators — the per-update cost should be flat in N.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 69.0 µs | 1.20 µs | 57.5× |
|       1000 | 585.1 µs | 1.40 µs | 418× |
|      10000 | 3.72 ms | 1.40 µs | 2654× |
|     100000 | 19.93 ms | 1.00 µs | 19935× |

## Operator fusion — `map → filter → map` chain

A three-stage linear chain (`MapRows` → `Filter` → `MapRows`) wired as three separate operators versus folded into one `MapFilterRows` pass — the pre- and post-fusion shapes for a run of adjacent Filter/Project plan nodes. Rows are `StructuralRow`s built as the structural compile path builds them; the filter keeps ~half the rows. Each step pushes the same N-row batch through the stateless chain. **Latency** is the median per-step time; **alloc/step** is bytes allocated per step (the unfused chain allocates an intermediate Z-set per stage plus a row for every input at the first map, before the filter can drop it).

| N (batch)  | Unfused       | Fused          | Speedup | Unfused alloc | Fused alloc | Alloc saved |
|-----------:|--------------:|---------------:|--------:|--------------:|------------:|------------:|
|        100 | 24.10 µs | 7.70 µs | 3.13× |   24.5 KB |    6.8 KB | 72% |
|       1000 | 216.50 µs | 75.60 µs | 2.86× |  221.2 KB |   62.9 KB | 72% |
|      10000 | 751.50 µs | 185.20 µs | 4.06× |    2.2 MB |  677.0 KB | 71% |
|     100000 | 5.42 ms | 1.79 ms | 3.03× |   20.9 MB |    6.3 MB | 70% |

## Multi-aggregate — `SUM / COUNT / MIN / MAX` over 100 groups

Stateful composite aggregator with retractions on every group-key hit. All four aggregators are now O(|delta|) per changed group: SUM / COUNT fold the delta into running state; MIN / MAX maintain a per-group sorted set of distinct values with positive net weight, indexed for O(log n) extremum lookup.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 130.1 µs | 2.80 µs | 46.5× |
|       1000 | 521.0 µs | 3.10 µs | 168× |
|      10000 | 2.72 ms | 3.70 µs | 734× |
|     100000 | 22.89 ms | 10.50 µs | 2180× |

## Joined GROUP BY — `SUM(amount)` per region over `orders ⋈ customers`

Inner join feeding a SUM aggregator. Per-update cost = probing the fixed customers trace + folding the one new order's amount into the per-region running sum.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 119.6 µs | 3.60 µs | 33.2× |
|       1000 | 774.5 µs | 3.70 µs | 209× |
|      10000 | 3.95 ms | 3.50 µs | 1127× |
|     100000 | 26.46 ms | 3.80 µs | 6964× |

## Joined GROUP BY — flat vs spine trace family

The same query and data as the Joined GROUP BY benchmark above, compiled once onto the flat dictionary traces and once onto the spine (LSM) traces via `CompileOptions { TraceFamily = TraceFamily.Spine }`. Both go through the same compiler, so the delta is the trace family alone: the spine pays a per-batch bloom + binary search on each probe where the flat trace does one dictionary lookup, in exchange for immutable batches that snapshot per-file and spill to disk. Operators: `IncrementalJoinOp` + `IncrementalAggregateOp` (flat) vs `SpineIncrementalJoinOp` + `SpineIncrementalAggregateOp`.

| N          | Flat batch    | Flat incr      | Spine batch   | Spine incr     | Spine incr vs flat |
|-----------:|--------------:|---------------:|--------------:|---------------:|-------------------:|
|        100 | 123.2 µs | 3.80 µs | 145.7 µs | 8.70 µs | 2.29× |
|       1000 | 733.9 µs | 3.90 µs | 852.0 µs | 25.10 µs | 6.44× |
|      10000 | 3.84 ms | 3.70 µs | 3.90 ms | 118.10 µs | 31.9× |
|     100000 | 24.68 ms | 3.60 µs | 24.95 ms | 183.80 µs | 51.1× |

## Joined GROUP BY (nullable `amount`) — same query shape

Identical query to the Joined GROUP BY benchmark, but `orders.amount` is nullable and ~10% of input rows have `NULL amount`. Exercises the typed pipeline's nullable-arg SUM path (Phase N4): per-row `HasValue` check, `DistinctNonNullRows` bookkeeping for the linear gate, and `Nullable<long>`-typed aggregate output slot. Compare to the non-null Joined GROUP BY to read off the per-row overhead of the nullable wrapper versus the non-null fast path.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 123.1 µs | 3.60 µs | 34.2× |
|       1000 | 834.5 µs | 3.70 µs | 226× |
|      10000 | 4.89 ms | 3.50 µs | 1396× |
|     100000 | 31.69 ms | 3.90 µs | 8125× |

## Joined GROUP BY (`EmittedEqualityCodec`) — same query shape

Identical query and circuit shape as the preceding Joined GROUP BY benchmark, but compiled with `EmittedEqualityCodec` — input rows, join outputs (via `MergeRows`), and group keys (via `ExtractKey`) are constructed as per-schema emitted subclasses of `StructuralRow`. Stays inside the existing pipeline (no generic lift). Measures the perf delta achievable from typed equality alone, before any field-access optimisation.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 220.1 µs | 5.20 µs | 42.3× |
|       1000 | 1.61 ms | 6.30 µs | 256× |
|      10000 | 9.64 ms | 11.70 µs | 824× |
|     100000 | 63.99 ms | 18.60 µs | 3440× |

## Joined GROUP BY (typed rows, hand-wired) — same query shape

Identical circuit to the preceding Joined GROUP BY benchmark, but hand-wired in the Core using `readonly record struct` rows — no `StructuralRow`, no `object?[]`, no SQL compilation: the ceiling the SQL compilers are measured against.

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|        100 | 124.2 µs | 4.00 µs | 31.1× |
|       1000 | 878.5 µs | 3.90 µs | 225× |
|      10000 | 4.24 ms | 4.20 µs | 1009× |
|     100000 | 13.82 ms | 4.00 µs | 3454× |

## Transitive closure — recursive CTE over a path graph

`WITH RECURSIVE reach AS (…)` over edges where the graph is a path `1→2→…→n`. Batch: fresh fixed-point, iterations × |R|. Incremental: the semi-naïve operator propagates only the rows newly reachable through the single added leaf edge (O(n) per update).

| N          | Batch         | Incremental    | Speedup |
|-----------:|--------------:|---------------:|--------:|
|         50 | 3.05 ms | 217.60 µs | 14.0× |
|        100 | 20.80 ms | 624.30 µs | 33.3× |
|        200 | 95.96 ms | 1.60 ms | 59.9× |
|        500 | 1.43 s | 10.79 ms | 132× |

## Pure-trace microbench — `ZSetTrace` vs `SpineZSetTrace`

Direct comparison between the flat in-place trace and the LSM-style `SpineZSetTrace` at varying trace sizes. Each trace is built by integrating N singleton `(key=i, weight=+1)` deltas, then four per-step ops are measured: `WeightOf` on a present key, `WeightOf` on an absent key, `Integrate` of a 1-key delta, and `Integrate` of a 100-key delta. Spine variants are tiered with 4 batches per level (default) and 2 batches per level (leveled-like). Probe latencies are amortised across 1000 back-to-back calls per sample (stopwatch resolution would otherwise round sub-100ns flat-trace probes to zero); `Integrate` is per-call because it mutates the trace.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | WeightOf(present)  | 27.2 ns | 547.8 ns | 446.9 ns |
|       1000 | WeightOf(absent)   | 21.8 ns | 412.2 ns | 325.5 ns |
|       1000 | Integrate(1)       | 100.0 ns | 500.0 ns | 1.30 µs |
|       1000 | Integrate(100)     | 13.60 µs | 15.90 µs | 17.40 µs |
|      10000 | WeightOf(present)  | 27.4 ns | 442.7 ns | 455.2 ns |
|      10000 | WeightOf(absent)   | 18.2 ns | 353.5 ns | 348.5 ns |
|      10000 | Integrate(1)       | 100.0 ns | 600.0 ns | 1.30 µs |
|      10000 | Integrate(100)     | 12.50 µs | 13.50 µs | 16.70 µs |
|     100000 | WeightOf(present)  | 8.0 ns | 67.2 ns | 59.7 ns |
|     100000 | WeightOf(absent)   | 8.3 ns | 39.4 ns | 35.7 ns |
|     100000 | Integrate(1)       | 0.0 ns | 100.0 ns | 300.0 ns |
|     100000 | Integrate(100)     | 1.70 µs | 3.20 µs | 2.00 µs |
|    1000000 | WeightOf(present)  | 7.9 ns | 114.1 ns | 97.2 ns |
|    1000000 | WeightOf(absent)   | 4.5 ns | 81.7 ns | 84.0 ns |
|    1000000 | Integrate(1)       | 0.0 ns | 100.0 ns | 300.0 ns |
|    1000000 | Integrate(100)     | 1.20 µs | 1.50 µs | 1.70 µs |

## Pure-trace microbench — `IndexedZSetTrace` vs `SpineIndexedZSetTrace`

Same shape, against the indexed trace that `IncrementalAggregateOp` and the join operators hold. The trace shape is one value per key (the join-trace pattern); `GroupFor` stands in for `WeightOf`.

| N          | Op                 | Flat        | Spine(N=4)  | Spine(N=2)  |
|-----------:|:-------------------|------------:|------------:|------------:|
|       1000 | GroupFor(present)  | 39.8 ns | 384.5 ns | 319.9 ns |
|       1000 | GroupFor(absent)   | 31.5 ns | 248.7 ns | 219.9 ns |
|       1000 | Integrate(1)       | 400.0 ns | 800.0 ns | 1.30 µs |
|       1000 | Integrate(100)     | 27.10 µs | 32.70 µs | 34.60 µs |
|      10000 | GroupFor(present)  | 48.0 ns | 314.0 ns | 349.0 ns |
|      10000 | GroupFor(absent)   | 29.5 ns | 214.4 ns | 228.3 ns |
|      10000 | Integrate(1)       | 100.0 ns | 200.0 ns | 400.0 ns |
|      10000 | Integrate(100)     | 7.00 µs | 10.60 µs | 11.20 µs |
|     100000 | GroupFor(present)  | 18.6 ns | 127.5 ns | 111.1 ns |
|     100000 | GroupFor(absent)   | 6.4 ns | 60.0 ns | 50.2 ns |
|     100000 | Integrate(1)       | 0.0 ns | 200.0 ns | 400.0 ns |
|     100000 | Integrate(100)     | 4.60 µs | 5.60 µs | 6.20 µs |
|    1000000 | GroupFor(present)  | 7.7 ns | 168.9 ns | 162.9 ns |
|    1000000 | GroupFor(absent)   | 4.9 ns | 105.4 ns | 101.8 ns |
|    1000000 | Integrate(1)       | 0.0 ns | 200.0 ns | 500.0 ns |
|    1000000 | Integrate(100)     | 4.70 µs | 5.30 µs | 6.20 µs |
## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine is tiered with 4 batches per level (default). **Spine·staged** adds the per-tick memtable (capacity 8,192, docs §13): integrate becomes an in-place dict merge, flushed to a sorted batch only every few thousand keys, so a churning `SELECT DISTINCT` stops rebuilding a batch per tick. `staged/spine` < 1 = the memtable speeds spine up; `staged/flat` near 1 = it reaches the flat dictionary.

| N          | Op           | Flat        | Spine       | Spine·staged | staged/spine | staged/flat |
|-----------:|:-------------|------------:|------------:|-------------:|-------------:|------------:|
|       1000 | new key      | 100.0 ns | 200.0 ns | 100.0 ns | 0.50× | 1.00× |
|       1000 | existing key | 100.0 ns | 200.0 ns | 100.0 ns | 0.50× | 1.00× |
|       1000 | bulk-10 mixed | 500.0 ns | 1.20 µs | 1.00 µs | 0.83× | 2.00× |
|      10000 | new key      | 100.0 ns | 200.0 ns | 100.0 ns | 0.50× | 1.00× |
|      10000 | existing key | 100.0 ns | 300.0 ns | 200.0 ns | 0.67× | 2.00× |
|      10000 | bulk-10 mixed | 500.0 ns | 1.50 µs | 1.00 µs | 0.67× | 2.00× |
|     100000 | new key      | 100.0 ns | 300.0 ns | 200.0 ns | 0.67× | 2.00× |
|     100000 | existing key | 100.0 ns | 300.0 ns | 200.0 ns | 0.67× | 2.00× |
|     100000 | bulk-10 mixed | 600.0 ns | 1.50 µs | 700.0 ns | 0.47× | 1.17× |
## Parallel scaling — throughput vs worker count

Data-parallel sharding (`ParallelCircuit`): `W` identical replicas, each processing a hash-partition of one large batch. The single-threaded incremental hot path is untouched — this is the throughput axis, so the metric is **rows/second on one big push**, not per-step latency. `W` is capped to the host's core count. Two shapes, measured apart:

- **Filter (exchange-free)** — no key-sensitive operator, so no shuffle and no per-tick barrier. Tests whether the driver + sharded I/O scale.
- **GROUP BY (exchange)** — an exchange re-shards by group key every tick through a full `Barrier(W)` + mailbox re-bucketing. Tests the exchange tax.
- **VARCHAR project (exchange-free)** — string columns make the boundary encode/decode the dominant cost, so it shows the parallel ingest/egest win.

Host: .NET 10.0.8, 14 cores.

### Filter (exchange-free) — `WHERE value > 500`

Batch of N=1,000,000 rows, all three phases timed apart. **Split** is ingest — boundary encode plus the hash shard into W buckets, now run on the worker threads (`ParallelIngestor`). **Step** is the parallel operator region. **Gather** is egest — when the compiler proves the output shards are key-disjoint, each worker decodes its own shard and the results concatenate (no serial combine); otherwise it falls back to the serial Z-set sum. Each column shows its own speedup vs W=1, so the ingest/egest scaling is visible alongside the operator scaling.

| W  | Split         | Split↑ | Step          | Step↑ | Gather        | Gather↑ |
|---:|--------------:|-------:|--------------:|------:|--------------:|--------:|
|  1 | 41.90 ms | 1.00× | 14.82 ms | 1.00× | 16.58 ms | 1.00× |
|  2 | 19.64 ms | 2.13× | 7.38 ms | 2.01× | 7.15 ms | 2.32× |
|  4 | 9.56 ms | 4.38× | 3.90 ms | 3.80× | 5.87 ms | 2.82× |
|  8 | 6.85 ms | 6.12× | 2.11 ms | 7.03× | 5.17 ms | 3.21× |
| 14 | 6.24 ms | 6.72× | 1.89 ms | 7.84× | 5.98 ms | 2.77× |

### GROUP BY (exchange) — `SUM(v), COUNT(*)` over 1000 groups

Batch of N=1,000,000 rows, all three phases timed apart. **Split** is ingest — boundary encode plus the hash shard into W buckets, now run on the worker threads (`ParallelIngestor`). **Step** is the parallel operator region. **Gather** is egest — when the compiler proves the output shards are key-disjoint, each worker decodes its own shard and the results concatenate (no serial combine); otherwise it falls back to the serial Z-set sum. Each column shows its own speedup vs W=1, so the ingest/egest scaling is visible alongside the operator scaling.

| W  | Split         | Split↑ | Step          | Step↑ | Gather        | Gather↑ |
|---:|--------------:|-------:|--------------:|------:|--------------:|--------:|
|  1 | 43.67 ms | 1.00× | 44.31 ms | 1.00× | 300.8 µs | 1.00× |
|  2 | 19.60 ms | 2.23× | 26.98 ms | 1.64× | 280.5 µs | 1.07× |
|  4 | 9.77 ms | 4.47× | 12.95 ms | 3.42× | 253.4 µs | 1.19× |
|  8 | 5.33 ms | 8.19× | 6.06 ms | 7.32× | 281.5 µs | 1.07× |
| 14 | 6.13 ms | 7.13× | 6.14 ms | 7.21× | 248.7 µs | 1.21× |

### VARCHAR project (exchange-free) — UTF-8 string rows

Batch of N=1,000,000 rows, all three phases timed apart. **Split** is ingest — boundary encode plus the hash shard into W buckets, now run on the worker threads (`ParallelIngestor`). **Step** is the parallel operator region. **Gather** is egest — when the compiler proves the output shards are key-disjoint, each worker decodes its own shard and the results concatenate (no serial combine); otherwise it falls back to the serial Z-set sum. Each column shows its own speedup vs W=1, so the ingest/egest scaling is visible alongside the operator scaling.

| W  | Split         | Split↑ | Step          | Step↑ | Gather        | Gather↑ |
|---:|--------------:|-------:|--------------:|------:|--------------:|--------:|
|  1 | 83.41 ms | 1.00× | 49.66 ms | 1.00× | 85.42 ms | 1.00× |
|  2 | 42.35 ms | 1.97× | 25.66 ms | 1.93× | 19.85 ms | 4.30× |
|  4 | 25.42 ms | 3.28× | 18.70 ms | 2.66× | 12.99 ms | 6.57× |
|  8 | 16.31 ms | 5.11× | 9.84 ms | 5.05× | 10.43 ms | 8.19× |
| 14 | 23.19 ms | 3.60× | 13.97 ms | 3.55× | 13.96 ms | 6.12× |

**Reading it.** The exchange-free filter's *Step* scales close to linearly in W — the replicas share nothing during a tick, so the driver and isolated operator state deliver the data-parallel win the model promises. The GROUP BY *Step* scales sublinearly: every tick pays one full `Barrier(W)` plus a rebuild of W mailbox buckets, so the exchange tax eats into the gain, though W>1 still beats W=1 on a batch this large. *Split* and *Gather* now scale too — the formerly serial ingest/egest are no longer the wall-clock floor. Ingest encodes and shards on the worker threads; egest, on the exchange-free shapes whose shards are key-disjoint, decodes per-worker and concatenates with no serial combine. The VARCHAR shape carries the widest rows, so it is where both wins show most (with `PARALLEL_STRINGS=string` its Split also pays the string → UTF-8 encode the engine does for .NET-string input). The GROUP BY *Gather* stays flat by design: its aggregate output is not disjoint (it falls back to the serial Z-set sum) but is tiny, so it never gates wall-clock. Allocation-heavy parallel ingest/egest needs Server GC to scale — workstation GC stops every worker on each collection and serializes exactly the work being parallelized.

## Interpretation

The number to read is the **Speedup** column — how many times faster an incremental update is than a cold recompute. Absolute numbers vary by host; the shape of the curve is the interesting signal.

- **Stateless (Filter, fusion).** Incremental cost is flat in N, about a microsecond, so the speedup grows linearly with the data. Fusing a `map → filter → map` chain into one `MapFilterRows` pass cuts per-step time by roughly 3× and allocation by about 70%, independent of N. That is what the compilers emit for any run of adjacent Filter/Project nodes.
- **Aggregates and joins.** Joined GROUP BY stays at a few microseconds per update across N, and its nullable and hand-wired typed-row variants track it closely: the SQL path is at the hand-wired ceiling. Multi-aggregate (SUM / COUNT / MIN / MAX) grows slowly with N because MIN / MAX keep a per-group ordered set. The `EmittedEqualityCodec` variant runs on the structural path only, so it is slower than the default, which compiles typed.
- **Transitive closure.** Batch grows roughly as n³ (n iterations over a closure of n² rows), and one leaf-edge insert costs O(n), so the speedup grows superlinearly with N.
- **Spine vs flat.** The flat traces win every per-step probe and integrate. The spine's case is immutable batches (per-file snapshots, spill), not step latency; flat stays the default (docs/decision-trace-family.md).
- **Parallel scaling.** Exchange-free work scales with W. An exchange adds a barrier per tick, so GROUP BY scales less well; see the section notes above.

