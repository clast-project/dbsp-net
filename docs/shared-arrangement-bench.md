# DbspNet — shared arrangement

Host: .NET 10.0.8, 24 cores.

## Shared arrangement — one shared index vs F private right traces

Gate for docs/design-row-representation.md §6.2 / §9 (Option 2, cross-operator shared arrangements). A relation `R` (wide rows) is joined by `F` downstream joins on the same key. **Unshared** = `F` plain inner joins, each maintaining its own right trace. **Shared** = one `Arrange(R)` + `F` shared-right joins reading that single integral. Same inputs, same join math, output verified identical. Each tick pushes a `D`-key delta into `R` and into each of the `F` left relations, alternating +1/-1 so state stays bounded. Times are median ns per **Step**; **Speedup** is unshared/shared (>1 = sharing wins). Both substrates are measured: **flat** (dictionary trace) and **spine** (LSM sorted-columnar trace — where the deduplicated maintenance is the expensive batch build, the §8.3 q4 cost). State: 20,000 keys/relation, group size 2.

Output equivalence (shared vs unshared) verified before timing on both substrates. 

### Flat substrate

| Fan-out F | D (keys/tick) | Unshared | Shared | Speedup |
|----------:|--------------:|---------:|-------:|--------:|
|         2 |           256 |  175.85 µs |  168.40 µs | 1.04× |
|         2 |          1024 |  752.50 µs |  717.95 µs | 1.05× |
|         2 |          4096 |    4.37 ms |    3.26 ms | 1.34× |
|         4 |           256 |  351.35 µs |  290.90 µs | 1.21× |
|         4 |          1024 |    1.87 ms |    1.30 ms | 1.43× |
|         4 |          4096 |    8.44 ms |    6.25 ms | 1.35× |
|         8 |           256 |  780.30 µs |  569.65 µs | 1.37× |
|         8 |          1024 |    4.68 ms |    2.89 ms | 1.62× |
|         8 |          4096 |   17.62 ms |   15.10 ms | 1.17× |

### Spine substrate

| Fan-out F | D (keys/tick) | Unshared | Shared | Speedup |
|----------:|--------------:|---------:|-------:|--------:|
|         2 |           256 |  324.60 µs |  322.70 µs | 1.01× |
|         2 |          1024 |    1.43 ms |    1.37 ms | 1.04× |
|         2 |          4096 |    7.30 ms |    7.08 ms | 1.03× |
|         4 |           256 |  653.75 µs |  597.10 µs | 1.09× |
|         4 |          1024 |    3.14 ms |    2.58 ms | 1.22× |
|         4 |          4096 |   13.85 ms |   12.21 ms | 1.13× |
|         8 |           256 |    1.32 ms |    1.14 ms | 1.15× |
|         8 |          1024 |    6.78 ms |    5.18 ms | 1.31× |
|         8 |          4096 |   31.42 ms |   26.53 ms | 1.18× |

## Reading

- **Sharing wins on both substrates, and the win grows with fan-out.** The unshared build maintains `R`'s delta in `F` private right traces; the shared build does it once, so the duplicated maintenance removed grows with `F`. Across repeated runs both substrates land ~1.0–1.1× at `F=2` and climb to ~1.2–1.4× at `F=8` (cells carry ~±0.1× jitter; read the trend in `F`).
- **Spine sharing is NOT bigger than flat sharing — it is comparable, often slightly smaller in ratio.** This refutes the §8.3-derived hypothesis. The reason is in the absolutes: at, say, `F=8, D=1024`, sharing removes a *similar absolute* per-tick cost on both substrates (~1.3–1.8 ms), but spine's total Step is ~1.5–2× the flat Step because its probe (`GroupForManySorted` across the batches) is heavier — so the same absolute saving is a *smaller fraction* of the spine baseline, and the ratio is diluted.
- **Why: the deduplicated cost is not the dominant per-tick term.** What sharing removes is `R`'s per-tick maintenance (the integrate — a dict merge on flat, a small batch build + amortised compaction on spine). What it does NOT remove is the per-consumer **probe** of `R_t` (each join still probes with its own left delta) plus the cross-product, output build, and left integrate. On the spine the probe is the larger term, so deduplicating the integrate moves the ratio less, not more.

## Verdict

Cross-operator shared arrangements work and are correct on both substrates (output verified identical to independent joins, here and in `SharedArrangementTests`). The realistic win is **modest and fan-out-scaling on both — ~1.0–1.4×**, and — contrary to the §8.3 expectation — the spine win is **not** larger than the flat win. The honest correction to §8.3: cross-operator sharing deduplicates a relation's per-tick *maintenance*, but the spine substrate's disadvantage on q4 is the per-tick *rebuild* paid even with no reuse — a cross-tick / amortisation problem, not a cross-operator one — and q4 has no shareable arrangement anyway (§6.2). So sharing is a real, broad-surface win for fan-out / star-schema / repeated-CTE shapes, **not** the lever that flips spine past flat. The reusable abstraction (`IArrangement` / `ISpineArrangement` + `Arrange` / `SpineArrange` + the shared-right joins) is in place on both substrates; the remaining follow-up is an **arrangement-CSE optimizer rule** so real SQL with a relation joined by the same key in ≥2 places routes through one arrangement automatically (today the feature is reachable only via the Core builder API).

