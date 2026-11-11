# TPC-DI batch 1 at twelve workers: where the time goes, and the plan

Status: design pass, 2026-11-10. Measurements below are at `2798958`. Nothing has been built yet.

## 0. Why, and the target

ivm-bench's GCI run (SF10 TPC-DI, twelve workers each) has DbspNet's batch 1 at 1:59 against
Feldera's 1:34, so we take 1.27× as long. On #95 (`f02030d`) it was 1:52 against 1:33. Batches 2 and 3
are already 5–8× faster than Feldera's. Batch 1 is the bulk historical load: it builds every
operator's state from nothing.

The goal is parity on batch 1, which means cutting about 21% of our time. Locally (SF=3, M4 Mac mini,
the `IvmMultiBatch` recipe in the ivm-bench-multicore notes) batch 1 drains in **~15.7 s**, so the
target is **~12.4 s**, provided the SF10 ratio carries over to SF=3. That is unverified, and §4 step 1
checks it.

What is ruled out before starting:

- **Representation alone.** docs/design-columnar-batch1.md §8–§9 found serial batch 1 at about
  parity with Feldera's single core. The gap is in how the parallel step uses twelve cores.
- **GC settings.** They are the host's choice; ivm-bench sets server GC with background GC off.
  Only changes to what the engine allocates or keeps alive count.
- **Reverting bounded-memory work.** The ~5% that batch 1 lost since `f02030d` is spread across
  §5.4–§5.41 and has no single cause (design-bounded-memory.md §5.42; the commit and tick sweeps of
  2026-11-10). It comes back through the levers below or not at all.

## 1. The decomposition (SF=3, W=12, three runs)

New instrument: `IVM_STEP_PROFILE=1` in `IvmMultiBatch` prints each worker's step split into
exchange split, barrier wait, gather, and everything else (`StepProfiler`). `IVM_GC_EVENTS=1` now
also names each GC's trigger.

| | per worker (mean) | share of step |
|---|---|---|
| step | 14.2–14.5 s | |
| operator work ("other") | 8.6–9.6 s | 61–67% |
| **barrier wait** | **2.9–3.1 s** | **21%** |
| exchange gather | 1.0–2.3 s | 7–16% |
| exchange split | 0.5–0.6 s | 4% |

- **Drain = ingest + step.** Ingest is ~1.4 s (Arrow read + decode ~0.3–0.5 s, push ~0.7–1.0 s).
  Output writes come after the drain.
- **Busy time (step − wait):** the fastest worker is at 10.7–10.9 s and the slowest at 12.8–13.1 s,
  a 2.2 s spread. Workers are imbalanced, but no single worker is the bottleneck.
- **GC pause, 3.3 s of the drain, stops every worker at once.** It lands in all four buckets above:

| GC | count | pause | what triggers it |
|---|---|---|---|
| gen0 | 11–13 | 1.3–1.6 s | small-object allocation |
| gen1 | 3–5 | 0.3–1.0 s | small-object allocation |
| gen2, blocking | 2–4 | 0.65–1.06 s | heap growth (alloc_soh) |
| gen2, blocking | 4 | 0.17–0.51 s | **large-object allocation** (alloc_loh): 2.3 GB, 0.9 GB of it block `byte[]` |

  The young GCs copy 18.5 GB from gen0 to gen1 and 7–9 GB from gen1 to gen2. A bulk load keeps most
  of what it builds, so it is copied up about twice (state-representation §9.15). Gen2 GCs are
  blocking because ivm-bench turns background GC off. In October gen2 was background and ~0 ms.
  Now it is about a third of all GC pause.

**By tick** (step time, medians of five runs, `DBSPNET_TICK_TIMES=1`). Each tick ingests one source.

| tick | source | step | GC pause inside |
|---|---|---|---|
| 18 | batch1_trade_type | ~4.5 s | ~1.6 s |
| 1 | staging_daily_market | ~3.7–4.1 s | ~0.6 s |
| 11 | batch1_finwire | ~1.8 s | ~0.36 s |
| 14 | batch1_status_type | ~1.7 s | ~0.5 s |
| other 16 | | ~2.4 s | |

Two ticks account for 60% of the step. Tick 18 is the trade fan-out: dim_trade, fact_holdings, the
trades_history joins. Tick 1 is daily_market's windows: the 52-week running MIN/MAX and
market_volatility's offsets over 1.28M rows.

**By operator kind** (CPU summed over workers, `DBSPNET_PROFILE=1`). This includes GC pauses and,
for exchanges, barrier wait. Per-operator times move between neighbours from run to run as pauses
land differently (§5 lessons), so only the kind totals are reliable.

| kind | CPU-s | | kind | CPU-s |
|---|---|---|---|---|
| ExchangeIndexOp ×81 + ExchangeOp ×67 | ~67 | | IncrementalAggregate ×13 | ~18.6 |
| WindowAggregate ×30 | ~23.6 | | WindowOffset ×8 | ~7.0 |
| ApplyOp ×280 | ~21.4 | | everything else | ~5 |
| IncrementalInnerJoin ×37 | ~19.4 | | **total** | **~162** |

The single heaviest operators are daily_market's two WindowAggregates (ops 383 and 386, ~17 s summed,
~1.4 s of wall per worker in tick 1). That is ~8–13 µs of CPU per row for a running window over a
bulk load.

## 2. Where 3.3 s could come from

Each candidate below is sized from §1, as an upper bound for the bucket it attacks.

| # | lever | bucket | ceiling | confidence |
|---|---|---|---|---|
| A | Reduce barrier wait: find the exchanges that wait, then fix skew or co-partition so those exchanges disappear | wait 3.0 s | ~1.5–2 s | unknown until attributed (step 2) |
| B | Bulk-load path for running window aggregates (daily_market) | tick 1 ~1.4 s/worker | ~0.7–1 s | medium: 8–13 µs/row looks algorithmic, not per-row |
| C | Fewer blocking gen2s: pool block buffers so they stop triggering LOH gen2s; check what grows gen2 | GC ~1.2 s gen2 | ~0.3–0.7 s | medium: only the LOH part is direct |
| D | Overlap source k+1's read and decode with tick k's step | ingest 0.3–0.5 s | ~0.3–0.5 s | high, small |
| E | Cheaper exchange gather: hand buckets on without a second hash and insert | gather 1.0–2.3 s | ~0.5 s | low: some of the range is GC |
| F | Per-row work in the tick-18 fan-out (joins, ApplyOp row rebuilds) | operator work | open | low until tick 18 is profiled alone |

A and B are the two that could each cover a third of the target. C and D are cheap and
independent. E and F wait on measurement.

Feldera's structure is the reference (feldera-source-comparison): one LSM trace per operator, no hash
map on the row path, and merging off the step thread. On a bulk load that means sorted batches built
once. Our hash-indexed Z-sets build the same state through a hash table and then freeze it. That is
the long-run per-row difference. A–D work inside the current design.

## 3. Rules for every step

- Every lever is an **interleaved A/B, at least five runs per arm**. The same binary moved ±0.8 s
  between sessions on 2026-11-10. Sequential sweeps with three runs did not resolve 0.5 s.
- **Views must be identical** (`VIEW` digests) on every run. Batches 2 and 3 must not regress
  (~0.1 s each).
- **Allocation (`drain_alloc_gib`) is deterministic.** A lever that claims an allocation or GC win
  must show it there first.
- Spilling stays off by default. Nothing here may change bounded-memory mode's behaviour. Run the
  forced suites for anything that touches blocks.

## 4. Plan, in order

1. **A local reference point.** Run Feldera's SF=3 batch 1 on the M4 at twelve workers, so we have a
   local ratio and not just CI's. It runs in ivm-bench's Docker setup, so Curt runs the commands.
   Record Feldera's own batch-1 duration. If it's practical, also find out whether Feldera steps
   once per source or once per batch. If once per batch, our 20 ticks are a structural difference
   worth costing.
2. **Attribute the wait (lever A).** Record barrier wait per exchange operator (per worker, per
   tick) in `StepProfiler`, keyed by the operator index the profile already prints. Rank exchanges by
   total wait. Then, for the top ones, check whether the upstream operator is skewed (one worker's
   share of rows, from `split_rows` and `gather_rows`) or evenly spread (per-barrier fixed cost).
   This decides between skew handling and fewer exchanges.
3. **Window bulk-load (lever B).** Microbenchmark ops 383 and 386 alone on the daily_market input:
   per-row CPU, and how much of it is affected-range recomputation that a first load doesn't need.
   Compare with what a load that sorts once per partition costs. Build only if the microbenchmark
   shows ≥2× on the operator.
4. **Cheap wins (C, D), in parallel with 2–3.** Pool block `byte[]` buffers, and confirm with
   `IVM_LOH_ALLOC=1` that alloc_loh gen2s drop. Prefetch the next source's read and decode while the
   current tick steps.
5. **Profile tick 18 alone (lever F)** once A–D have landed, then re-rank what's left.
6. **Re-pin ivm-bench** when the local A/B shows a net gain against `f02030d`, the current upstream
   pin. Drop the Dockerfile's `git submodule update --init` step in the same commit.

## 5. Lessons this pass rests on

- Per-operator wall time in a parallel run follows where GC pauses land. Two IntegratedViews went
  from ~55 ms to ~550 ms in two of three runs with no code change. Use kind totals, allocation, or
  per-tick wall.
- `DBSPNET_STREAM_RELEASE_FIRST_TICK=0` does not recover the 5% since `f02030d`. R0's real cost is
  ~110 ms in tick 0, the reader scan (§5.7).
- The engineered-wood 0.4.0 package (Release, 70 commits past the old submodule) costs ~1% of
  batch 1 against the submodule build.
