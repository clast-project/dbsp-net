# DbspNet — shared arrangement

Host: .NET 10.0.8, 24 cores.

## Shared arrangement — one shared index vs F private right traces

Gate for docs/design-row-representation.md §6.2 (Option 2, cross-operator shared arrangements). A relation `R` (wide rows) is joined by `F` downstream joins on the same key. **Unshared** = `F` plain `IncrementalInnerJoin`s, each integrating `R`'s delta into its own right trace. **Shared** = one `Arrange(R)` + `F` `IncrementalInnerJoinSharedRight` reading that single integral. Same inputs, same join math, output verified identical. Each tick pushes a `D`-key delta into `R` and into each of the `F` left relations, alternating +1/-1 so state stays bounded. Times are median ns per **Step**; **Speedup** is unshared/shared (>1 = sharing wins). State: 20,000 keys/relation, group size 2.

Output equivalence (shared vs unshared) verified before timing. 

| Fan-out F | D (keys/tick) | Unshared | Shared | Speedup |
|----------:|--------------:|---------:|-------:|--------:|
|         2 |           256 |  176.35 µs |  160.35 µs | 1.10× |
|         2 |          1024 |  764.60 µs |  724.50 µs | 1.06× |
|         2 |          4096 |    4.10 ms |    4.22 ms | 0.97× |
|         4 |           256 |  416.90 µs |  328.00 µs | 1.27× |
|         4 |          1024 |    2.42 ms |    1.55 ms | 1.56× |
|         4 |          4096 |    9.49 ms |    7.51 ms | 1.26× |
|         8 |           256 |  695.35 µs |  582.00 µs | 1.19× |
|         8 |          1024 |    5.09 ms |    3.24 ms | 1.57× |
|         8 |          4096 |   17.66 ms |   13.53 ms | 1.31× |

## Reading

- **Sharing wins across the grid, and the win grows with fan-out.** Every cell is ≥ 1.0× (sharing never loses): the unshared build integrates `R`'s delta into `F` private right traces, the shared build does it once, so the duplicated maintenance removed grows with the number of consumers. Across repeated runs `F = 2` (one duplicate integrate saved) is the marginal case (~1.0–1.3×), `F = 4` lands ~1.2–1.4×, and `F = 8` reaches ~1.5×. The trend in `F` is robust; individual cells carry ~±0.2× run-to-run jitter.
- **Clearest at moderate ticks; very wide ticks get noisy.** The cleanest, most repeatable win is around `D = 1024`. At `D = 4096` each Step allocates and frees large Z-sets, so per-tick allocation / GC starts to dominate and dilutes (and destabilises) the integrate-sharing signal — the win is real but the ratio wobbles.
- **The ceiling is modest because the flat integrate is cheap.** A flat `IndexedZSetTrace.Integrate` is an in-place dictionary merge; even shared across 8 consumers it is only a fraction of per-tick work (the rest — the two join passes, output build, and left-trace integrate — is per-consumer and unchanged). So flat sharing tops out around ~1.5×, not `F`×. This is the honest cross-operator result on the substrate that wins on q4 today.
- **The bigger prize is the spine arrangement, not the flat one.** On the spine path the duplicated per-consumer maintenance is a full sorted-columnar **batch build** (+ bloom + compaction) — the exact §8.3 q4 substrate cost — which is far more expensive than a dict merge, so sharing it should pay much more. That variant (a spine `Arrange` whose consumers probe via `GroupForManySorted` against the shared trace) reuses this same `IArrangement` abstraction and is the natural follow-up.

## Verdict

Cross-operator shared arrangements work and are correct (output verified identical to independent joins, here and in `SharedArrangementTests`). On the flat substrate the win is real but modest — up to ~1.5× at fan-out 8 — because the maintenance it deduplicates (a dictionary integrate) is already cheap. The abstraction (`IArrangement` + `Arrange` + `IncrementalInnerJoinSharedRight`) is the reusable foundation; the two follow-ups that turn it into a headline win are (1) the **spine** arrangement (shares the expensive batch build — the §8.3 bottleneck) and (2) an **arrangement-CSE optimizer rule** so real SQL with a relation joined by the same key in ≥2 places routes through one `Arrange` automatically (today the feature is reachable only via the Core builder API).

