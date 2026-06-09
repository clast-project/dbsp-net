# DbspNet — distinct (flat vs spine vs staged)

Host: .NET 10.0.8, 24 cores.

## DistinctOp — flat vs spine (per-Step latency)

Per-step latency of `DistinctOp` and `SpineDistinctOp` after the trace is pre-loaded with N distinct integer keys. Three delta shapes: a single **new key** (probe miss + integrate), a single **existing key** at +1 (probe hit + no-op integrate), and a **bulk-10** delta of mixed new + existing keys (probe + integrate work scaled 10×, swamping the per-step overhead that obscures the trace cost in singleton-delta measurements). Spine is tiered with 4 batches per level (default). **Spine·staged** adds the per-tick memtable (capacity 8,192, docs §13): integrate becomes an in-place dict merge, flushed to a sorted batch only every few thousand keys, so a churning `SELECT DISTINCT` stops rebuilding a batch per tick. `staged/spine` < 1 = the memtable speeds spine up; `staged/flat` near 1 = it reaches the flat dictionary.

| N          | Op           | Flat        | Spine       | Spine·staged | staged/spine | staged/flat |
|-----------:|:-------------|------------:|------------:|-------------:|-------------:|------------:|
|       1000 | new key      | 500.0 ns | 2.70 µs | 500.0 ns | 0.19× | 1.00× |
|       1000 | existing key | 500.0 ns | 2.80 µs | 500.0 ns | 0.18× | 1.00× |
|       1000 | bulk-10 mixed | 3.10 µs | 19.60 µs | 3.30 µs | 0.17× | 1.06× |
|      10000 | new key      | 500.0 ns | 4.20 µs | 800.0 ns | 0.19× | 1.60× |
|      10000 | existing key | 500.0 ns | 4.10 µs | 800.0 ns | 0.20× | 1.60× |
|      10000 | bulk-10 mixed | 2.60 µs | 31.90 µs | 5.70 µs | 0.18× | 2.19× |
|     100000 | new key      | 200.0 ns | 400.0 ns | 200.0 ns | 0.50× | 1.00× |
|     100000 | existing key | 100.0 ns | 400.0 ns | 200.0 ns | 0.50× | 2.00× |
|     100000 | bulk-10 mixed | 300.0 ns | 1.70 µs | 600.0 ns | 0.35× | 2.00× |
