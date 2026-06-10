# DbspNet — surrogate-key microbench

Host: .NET 10.0.9, 24 cores.

## Surrogate-key microbench — whole-row hash vs interned int

Microbench for docs/design-row-representation.md §14.6. Models the flat aggregate's per-tick group rebuild (`ZSetBuilder.From`, a verified per-entry re-hash). The **struct** path keys a `Dictionary` on the whole row (the emitted typed row's multi-field `GetHashCode`); the **surrogate** path interns each distinct row to an `int` once, then keys a `Dictionary<int,…>`. A rebuild does one `d[key]=w` insert per row (mirroring `ZSetBuilder.From`) plus one `TryGetValue` scan (mirroring the aggregator read) — two hashes per row per rebuild.

Row **widths**: `W2`/`W4`/`W8` are 2/4/8 `long` columns; `WStr` is 3 `long`s + a `string` (a Nexmark-bid-like row — string hashing is the expensive case). Surrogate totals **include** the one-time intern, so the speedup already pays for interning.

### Crossover sweep — 1,000 distinct rows, rebuilt + scanned R times

`struct`/`surr` are median ns for the whole (R rebuilds of 1,000 rows); **Speedup** = struct/surr (>1 = surrogate wins). The first R where Speedup ≥ 1.0 is the crossover R\* for that width.

#### Width W2

| R (re-touches) | struct | surrogate | Speedup |
|---------------:|-------:|----------:|--------:|
|              1 |   114.50 µs |   164.50 µs | 0.70× |
|              2 |   149.50 µs |   167.10 µs | 0.89× |
|              4 |   244.80 µs |   212.10 µs | 1.15× |
|              8 |   466.90 µs |   317.00 µs | 1.47× |
|             16 |   876.00 µs |   550.20 µs | 1.59× |
|             32 |     1.66 ms |   997.90 µs | 1.66× |
|             64 |     3.17 ms |   368.60 µs | 8.61× |

#### Width W4

| R (re-touches) | struct | surrogate | Speedup |
|---------------:|-------:|----------:|--------:|
|              1 |   119.10 µs |   120.50 µs | 0.99× |
|              2 |   167.10 µs |   103.90 µs | 1.61× |
|              4 |   262.60 µs |   117.40 µs | 2.24× |
|              8 |   495.90 µs |   124.90 µs | 3.97× |
|             16 |   966.50 µs |   183.70 µs | 5.26× |
|             32 |     1.82 ms |   289.10 µs | 6.29× |
|             64 |     3.63 ms |   493.10 µs | 7.36× |

#### Width W8

| R (re-touches) | struct | surrogate | Speedup |
|---------------:|-------:|----------:|--------:|
|              1 |   164.30 µs |   160.40 µs | 1.02× |
|              2 |   240.70 µs |   143.80 µs | 1.67× |
|              4 |   406.20 µs |   150.30 µs | 2.70× |
|              8 |   790.30 µs |   171.00 µs | 4.62× |
|             16 |     1.55 ms |   210.10 µs | 7.38× |
|             32 |     3.05 ms |   321.10 µs | 9.48× |
|             64 |     5.93 ms |   481.10 µs | 12.3× |

#### Width WStr

| R (re-touches) | struct | surrogate | Speedup |
|---------------:|-------:|----------:|--------:|
|              1 |   154.90 µs |   147.40 µs | 1.05× |
|              2 |   240.90 µs |   138.30 µs | 1.74× |
|              4 |   424.10 µs |   141.70 µs | 2.99× |
|              8 |   801.20 µs |   164.10 µs | 4.88× |
|             16 |     1.54 ms |   204.50 µs | 7.52× |
|             32 |     3.00 ms |   299.60 µs | 10.0× |
|             64 |     8.54 ms |   467.90 µs | 18.2× |

### Growing group — one group grows to K, rebuilt every tick (O(K²))

The actual aggregate shape: a group accumulates one new row per tick and is rebuilt each tick, so a row added early is re-hashed ~K times. `struct`/`surr` are median ns for the **whole K-tick growth**; surrogate interns each new row once. This is the q4 per-auction-bid case (rows are distinct, so §6.5's 'hashed once' assumption is wrong — incremental maintenance re-hashes them K times).

| Width | K | struct | surrogate | Speedup |
|:------|--:|-------:|----------:|--------:|
| W2 | 16 |     2.00 µs |     1.90 µs | 1.05× |
| W4 | 16 |     8.70 µs |     3.50 µs | 2.49× |
| W8 | 16 |     4.40 µs |     2.00 µs | 2.20× |
| WStr | 16 |     7.00 µs |     5.00 µs | 1.40× |
| W2 | 64 |    21.50 µs |    16.90 µs | 1.27× |
| W4 | 64 |    66.90 µs |    21.70 µs | 3.08× |
| W8 | 64 |    44.20 µs |    18.90 µs | 2.34× |
| WStr | 64 |    65.70 µs |    19.80 µs | 3.32× |
| W2 | 256 |   222.80 µs |   155.70 µs | 1.43× |
| W4 | 256 |   320.10 µs |   157.80 µs | 2.03× |
| W8 | 256 |   426.40 µs |   166.80 µs | 2.56× |
| WStr | 256 |   772.60 µs |   171.00 µs | 4.52× |
| W2 | 1024 |     2.96 ms |     2.28 ms | 1.30× |
| W4 | 1024 |     4.68 ms |     2.31 ms | 2.03× |
| W8 | 1024 |     9.13 ms |     2.49 ms | 3.66× |
| WStr | 1024 |    13.12 ms |     2.47 ms | 5.30× |

