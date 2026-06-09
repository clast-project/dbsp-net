# DbspNet — shared arrangement (SQL optimizer rule)

Host: .NET 10.0.8, 24 cores.

## Arrangement CSE on real SQL — F facts joined to one wide `dim`

Gate for the arrangement-CSE optimizer rule (docs §9.6). The query is a `UNION ALL` of `F` inner joins, each joining a fact table to a shared wide `dim` on the same key. **Unshared** and **Shared** compile on the same structural path with the same codec, differing only in `CompileOptions.ShareArrangements`: when on, the compiler detects `dim` is the right input of ≥2 joins on the same key and builds ONE `Arrange` / `SpineArrange` both joins read, instead of a private right trace each. Output verified identical. Each tick pushes a delta into `dim` and each fact; times are median ns per **Step**, speedup = unshared/shared. State 20,000 keys/table, 1024 keys/tick.

Output equivalence (shared vs unshared) verified on both substrates. 

### Flat substrate

| Fan-out F | Unshared | Shared | Speedup |
|----------:|---------:|-------:|--------:|
|         2 |   36.64 ms |   36.16 ms | 1.01× |
|         4 |   62.51 ms |   60.56 ms | 1.03× |
|         8 |  113.89 ms |  109.52 ms | 1.04× |

### Spine substrate

| Fan-out F | Unshared | Shared | Speedup |
|----------:|---------:|-------:|--------:|
|         2 |   38.84 ms |   38.05 ms | 1.02× |
|         4 |   66.81 ms |   65.43 ms | 1.02× |
|         8 |  118.08 ms |  115.00 ms | 1.03× |

## Verdict

The optimizer rule routes real SQL through a single shared arrangement and the results are identical to the unshared compile (also in `ArrangementSharingTests`). The end-to-end win is small (~1–6%, growing with fan-out) — smaller than the operator-level figure (§9.5, ~1.5× at F=8) — because at the query level the deduplicated `dim` maintenance is a small fraction of per-step work: input row encoding, each fact's re-index, the UNION, projection, and output build are all per-branch and unchanged. The rule's value is making cross-operator sharing reachable from SQL and proving it correct end-to-end, not a step change. Off by default; enabling it forces the structural compile path.

