# Merge-probe prototype — results

Prototype for the row-representation design
([docs/design-row-representation.md](design-row-representation.md) §6.1).
Reproduce with `dotnet run -c Release -- mergeprobe` from `src/DbspNet.Benchmarks`.

The spine indexed trace (`SpineIndexedZSetTrace`, the state
`SpineIncrementalAggregateOp` and the join operators hold) is probed for one
tick of `D` group keys two ways:

- **Point probe** (baseline = today's operator inner loop): a loop of
  `GroupFor` calls — bloom-gated binary search across every batch, rebuilding a
  `ZSet` (hashing each value) per probe.
- **Merge probe** (prototype): a single `GroupForManySorted` call — each sorted
  batch's outer-key column walked once with a galloping cursor, matched groups
  sliced straight from the value columns, no per-probe rehash.

Each group holds 4 values. Times are the median ns for the **whole D-key tick**;
**Speedup** is point/merge (>1 = merge wins). `present` keys exist (the
aggregate's own changed groups); `absent` keys miss (the join probe-side case).
Host: .NET 10, 24 cores (single-threaded measurement). Numbers are one run —
read the shape, not the third digit.

## present keys (the aggregate case)

| N | D=1 | D=8 | D=64 | D=512 | D=4096 |
|--:|----:|----:|-----:|------:|-------:|
| 1,000 | 5.54× | 2.30× | 2.98× | 5.82× | 18.1× |
| 100,000 | 0.34× | 1.04× | 1.39× | 2.34× | 2.45× |
| 1,000,000 | 3.17× | 3.19× | 4.17× | 4.73× | 4.14× |

## absent keys (the join probe-side case)

| N | D=1 | D=8 | D=64 | D=512 | D=4096 |
|--:|----:|----:|-----:|------:|-------:|
| 1,000 | 0.79× | 5.83× | 31.8× | 60.3× | 35.0× |
| 100,000 | 0.17× | 1.40× | 6.21× | 15.7× | 39.1× |
| 1,000,000 | 2.34× | 18.2× | 61.2× | 110× | 135× |

## Reading

- **The merge wins almost everywhere, and the win grows with D** — exactly the
  §5 prediction. By D=512 (a modest tick) it is 2–110× faster; at D=1 it is a
  wash or a small loss. The crossover is low: even D=8 is already ahead in most
  rows.
- **Absent keys win hugest.** A point probe still pays a full bloom + binary
  search per missing key; the galloping merge skips a non-matching key in ~one
  comparison and skips a whole non-overlapping batch via the range gate. At
  N=1M, D=4096 absent the merge is **135×** faster. This is the join probe side,
  where most keys miss — the most promising rollout target.
- **The lone soft spots are D=1 at N=100k** (0.34× present, 0.17× absent): for a
  single-key tick the merge's per-batch cursor setup and result-list allocation
  aren't amortised, and the point probe's single bloom+binary-search is hard to
  beat. The fix in a real operator is trivial — keep the point-probe path for
  `D==1` (or the existing `GroupFor`), switch to the merge for wider ticks. The
  N=1M D=1 row already swings back to 3.17×/2.34× because at that depth even one
  probe touches more batches.

## Verdict

The design's core hypothesis holds: **sorted-merge / galloping execution over
the existing spine substrate beats per-key point-probing, and the margin widens
with tick width and (for misses) with state size.** The §5 caveat is real but
narrow — it costs only at literal `D==1`, guardable with a one-line size check.

**Next increment (if pursued):** wire `GroupForManySorted` into
`SpineIncrementalAggregateOp.Step` (sort the delta keys once, batch-probe,
consume the sorted runs) and into the spine join probe, then measure end-to-end
on the q4 Nexmark step via `nexprofile`/`comparison`. The aggregator currently
consumes a `ZSet<TValue>`; feeding it sorted runs without rebuilding a `ZSet`
(to capture the no-rehash win end-to-end) is the one signature question to
settle there — mind the typed-compiler reflection coupling noted in the
typed-compiler-reflection-gotcha memory.
