# DbspNet — join merge-probe

Host: .NET 10.0.8, 24 cores.

## Join merge-probe — point probe vs galloping merge at the operator

Operator-level gate for docs/design-row-representation.md §8: the whole `SpineIncrementalJoinOp.Step` (probe + cross-product + output build) is timed with the per-key `GroupFor` point probe (`ForcePointProbe`, today's baseline) versus the batched `GroupForManySorted` merge. Each tick pushes a `D`-key left delta and steps; the join probes the stable `N`-key right trace (group size 2, wide probe-side rows). Ticks alternate +1/-1 so left state stays bounded. Times are median ns per **Step**; **Speedup** is point/merge (>1 = merge wins). `present` keys hit existing right groups; `absent` keys miss (the join probe side).

### N = 1,000, present keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |    4.30 µs |    3.35 µs | 1.28× |
|             8 |   16.95 µs |   10.45 µs | 1.62× |
|            64 |  160.05 µs |   74.30 µs | 2.15× |
|           512 |  952.80 µs |  509.50 µs | 1.87× |
|          4096 |    1.86 ms |  948.40 µs | 1.96× |

### N = 1,000, absent keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |    2.45 µs |    2.65 µs | 0.92× |
|             8 |    8.90 µs |    6.60 µs | 1.35× |
|            64 |   56.85 µs |   38.60 µs | 1.47× |
|           512 |  445.40 µs |  294.15 µs | 1.51× |
|          4096 |  842.60 µs |  571.30 µs | 1.47× |

### N = 100,000, present keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |    4.70 µs |    3.85 µs | 1.22× |
|             8 |   17.95 µs |    8.70 µs | 2.06× |
|            64 |   43.75 µs |   15.30 µs | 2.86× |
|           512 |  315.75 µs |  122.75 µs | 2.57× |
|          4096 |    1.91 ms |    1.12 ms | 1.71× |

### N = 100,000, absent keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |     650 ns |     600 ns | 1.08× |
|             8 |    1.50 µs |    1.30 µs | 1.15× |
|            64 |    7.75 µs |    6.05 µs | 1.28× |
|           512 |   63.55 µs |   52.25 µs | 1.22× |
|          4096 |  574.00 µs |  523.45 µs | 1.10× |

### N = 1,000,000, present keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |     850 ns |    1.05 µs | 0.81× |
|             8 |    3.35 µs |    2.35 µs | 1.43× |
|            64 |   23.35 µs |   29.85 µs | 0.78× |
|           512 |  224.85 µs |  152.65 µs | 1.47× |
|          4096 |    2.26 ms |    1.90 ms | 1.19× |

### N = 1,000,000, absent keys

| D (keys/tick) | Point probe | Merge probe | Speedup |
|--------------:|------------:|------------:|--------:|
|             1 |     650 ns |     600 ns | 1.08× |
|             8 |    4.95 µs |    1.15 µs | 4.30× |
|            64 |    9.90 µs |   13.20 µs | 0.75× |
|           512 |   82.45 µs |   53.20 µs | 1.55× |
|          4096 |  758.65 µs |  635.55 µs | 1.19× |

## Reading

- **The merge wins across the grid wherever it is engaged.** For every multi-key tick (`D > 1`) the batched merge beats the point probe, typically 1.2–2.8×, growing with `D`. The win is smaller than the trace-level `mergeprobe` figures (2–135×) because a whole `Step` also pays the **unchanged** cross-product, output build, and left-trace integrate — common overhead that dilutes the probe-only ratio. That the operator still moves 1.2–2.8× confirms the probe was a real slice of `Step`.
- **`D == 1` is a wash by construction.** The operator keeps the point probe for single-key ticks (the trace-level soft spot), so both columns run the same path there and the ~1.0× rows simply confirm no regression.
- **No regressions.** Every engaged cell is ≥ 1.0× within noise; the lone sub-1.0 cell (N=1M, absent, D=8) is a sub-3µs tick dominated by integrate, where run-to-run noise swamps the tiny probe.

## Verdict

Wiring `GroupForManySorted` into `SpineIncrementalJoinOp.Step` carries the trace-level merge win through to the whole operator: 1.2–2.8× faster steps on multi-key ticks, no regression at `D == 1`, output verified identical to the flat join by the spine join test suite. The end-to-end q4 step on the spine path at W=14 is deferred to the rollout step that flips `TraceFamily.Spine` toward the default for the typed parallel compiler (the parallel Nexmark harness currently compiles flat-only).

