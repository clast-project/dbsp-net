# TPC-DI batch 1 at twelve workers: where the time goes, and the plan

Status: in progress. §1–§2 were measured at `2798958`. Broadcast joins (§4a), smallest-first input
order (§4c) and byte-level string functions (§4d) have shipped; batch 1 is now within ~5% of Feldera locally.

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

## 4d. The finwire parse: byte-level TRIM, SUBSTRING and casts (2026-11-11)

§4c's guess, a regex compiled per row, was wrong: a constant `RLIKE` pattern is already compiled
once. The cost was in the string functions. `finwire_financial` cuts ~17 fields out of each
~550-byte line with `TRIM(SUBSTRING(line, …))`, then `CAST`s most of them.

- **`TRIM`** built a `HashSet<Rune>` and two lists holding every code point of its input on every
  call. Now it trims ASCII characters (plain `TRIM`, or an all-ASCII list) on the bytes. That's exact
  for any input: in UTF-8 a byte below 0x80 is always a whole code point, and every byte of a
  multi-byte code point is 0x80 or above.
- **`SUBSTRING`** walked the line from the start to count code points, three times per call. On
  all-ASCII strings (checked vectorised) positions are now byte offsets.
- **VARCHAR → INT/BIGINT/REAL/DOUBLE/DATE casts** decoded the bytes into a .NET `string` first. They now
  parse the UTF-8 bytes with the same `NumberStyles`. DATE reads the strict `yyyy-MM-dd` form, and
  anything the fast parse rejects falls back to the string path, so every accepted value and every
  error stays the same.

The parse operator went from 4.9 CPU-s and 1,536 MiB to 1.6 CPU-s and 273 MiB. `StringFastPathTests`
checks each fast path against the path it skips on random input, including multi-byte and malformed
UTF-8; four mutants each fail it.

The rows are spread evenly across workers, so the wall-clock gain is small. Three arms over five
rotated rounds, all three batches, views identical in all 15 runs (broadcast ≤ 1,000 and smallest
first wherever the build has them):

| | batch-1 drain | output | drain + output | alloc | batches 2 / 3 (with output) |
|---|---|---|---|---|---|
| `f02030d` (ivm-bench pin) | 15.42 s | 1.43 s | 16.85 s | 54.87 GiB | 0.12 / 0.13 s |
| `bb7a0ac` (§4c) | 13.04 s | 1.67 s | 14.72 s | 46.56 GiB | 0.13 / 0.13 s |
| **string fixes** | **12.92 s** | 1.63 s | **14.61 s** | **45.31 GiB** | 0.12 / 0.11 s |

Against the pin: drain −16%, drain + output −13%, allocation −17%. An earlier two-arm A/B that always
ran the string build second showed it 1.3% slower. With the order rotated it's level or ahead, so
read sub-2% wall differences from fixed-order pairs as noise.

## 4c. Input order: load the dimensions first (2026-11-11)

Chasing the financials skew left over from §4a turned up a different cause. `financials` left-joins
`finwire_financial` to `companies` twice: on CIK for half the FIN records, and on company name for
the other half. Each half has a NULL key in the other join, and the CIK join never matches anyway
(`CAST(81 AS VARCHAR)` is `'81'`, the company IDs are `'0000000081'`). So ~49k rows get a NULL
`company_id`. That is the same SQL Feldera runs. But per-op tracing showed the window ops getting all
98,576 rows in **one** partition on one tick, then 102k rows of retractions and re-inserts on the
next. `companies` was still empty when `finwire` arrived.

That's the input order. Batch 1 ingests one source version per tick, in spec order. ivm-bench's spec
lists `staging_*` first and then `batch1_*` alphabetically, so `industry`, `status_type`, and the
five-row `trade_type` arrive at ticks 13, 14 and 18, after 2.3M fact rows. Until a dimension arrives,
every left join pads the facts and every inner join builds fact-sized state. When it lands, all of
that is retracted and recomputed, along with the windows and aggregates downstream. Ticks 14 and 18
(1.9 s and 3.1 s) were mostly this.

**Built (server, default on).** `DbspNetEngine` reads inputs smallest first, by the same Delta-log
counts broadcast uses (now read once). Inputs of unknown or zero size go last, in spec order.
`DBSPNET_INPUT_ORDER=spec` keeps the spec's order (`InputOrder`). The order isn't recorded with a
checkpoint: it changes no result and no state layout, and the tick journal replays by source name.
`DeployResult.InputOrder` reports it, and `IvmMultiBatch` follows the same default
(`IVM_INPUT_ORDER=spec` to compare).

Interleaved A/B, five runs per arm, broadcast ≤ 1,000 in both, all three batches, views identical in
all ten runs and to §4a's:

| | batch-1 drain median | output | drain + output | alloc | batches 2 / 3 |
|---|---|---|---|---|---|
| spec order | 14.30 s | 1.48 s | 15.76 s | 53.74 GiB | ~0.10 / 0.11 s |
| **smallest first** | **12.92 s (−9.7%; 5 of 5 pairs)** | 1.66 s | **14.43 s (−8.4%)** | **46.58 GiB (−13%)** | unchanged |

Against Feldera's 13.9 s (§4b), batch 1 is now 1.04× locally, down from 1.24× before §4a. Output
writes went up ~0.2 s; that's unexplained.

**Tried, not built: one engine step per batch.** Pushing every source and then stepping once gave a
drain of 11.74 s and 42.3 GiB in one run, with identical views. Drain plus output (13.9 s) was about
the same as smallest first, though, because output rose to 2.2 s. Batches 2 and 3 doubled to ~0.25 s.
And one tick holding the whole batch works against the bounded-memory work, whose spilling assumes a
tick per source. Smallest first gets most of the gain and keeps the per-source ticks.

**Still open in financials:** the NULL-`company_id` window partition (~0.3 s, once, on one worker)
comes from the SQL itself. Op 470, the `finwire_financial` parse, takes 4.9 CPU-s and 1.5 GiB for
102k lines (~48 µs and ~15 KB per line). It's balanced across workers, but that much per row looks
like a per-call cost, such as compiling the `RLIKE` regex on every call.

## 4b. Step 1 result: Feldera at SF=3 on the M4 (2026-11-11)

Feldera alone through ivm-bench's harness (`BENCHMARK_RUNS=3`, batches 100/1/1, the Feldera profile's
twelve workers with `adaptive_joins`, `pipeline-manager:latest`, inside Docker Desktop's Linux VM).
Its batch time runs from `/resume` until every output connector has finished writing, so it includes
output writes. Our `drain_ms` does not; the comparable figure for us is `drain_ms + output_ms`.

| | batch 1 | batches 2 / 3 |
|---|---|---|
| Feldera (3 runs) | 14.7, 13.7, 13.4 s (mean **13.9 s**); 113–132 CPU-s | 2.07 / 2.04 s |
| DbspNet, no broadcast (§4a, 5 runs) | 15.7 + 1.5 output = **~17.2 s** (1.24×) | ~0.1 + ~0.03 s |
| DbspNet, broadcast ≤ 1,000 (5 runs) | 14.4 + 1.4 output = **~15.9 s** (1.14×) | unchanged |

- **The SF10 ratio carries over.** Without broadcast we are 1.24× locally against 1.27× in CI, so the
  local loop is a fair proxy for GCI.
- **Broadcast closes about a third of the gap.** What's left is ~2.0 s at SF=3, so the target is
  ~13.9 s end to end, or ~12.5 s drain at our current 1.4 s of output writes.
- **Caveats.** The arms ran natively on macOS versus in a Linux VM, and Feldera's end time comes from
  polling, so read differences under ~0.5 s as noise. Batches 2/3 are dominated by Feldera's per-batch
  overhead, not its operator work.
- **Ticks per batch is still open.** The harness log doesn't show whether Feldera steps once per
  source or once per batch.

## 4a. Step 2 result: the wait is skew from tiny dimensions (2026-11-10)

New instrument: under `DBSPNET_PROFILE=1` the batch profile now prints **exchanges by barrier wait**.
That is each exchange's wait summed over workers, plus the max/mean of rows it sent and received per
worker (`OperatorProfile.WaitMs` / `SplitRows` / `GatherRows`, recorded by `StepProfiler` for whichever
operator a profiled circuit is stepping).

Total wait: 36.2 s summed over workers (3.0 s each), concentrated in two places:

- **trades_history: ~16.2 s, 45%.** The view joins 982k trades to `reference_trade_type` (5 rows) on
  `t_tt_id` and to `reference_status_type` (6 rows) twice. Hash-sharding the fact side on a key with 5–6
  values puts it on at most 5–6 workers. Exchange 218 lands its 982k rows at 11.37× the mean on one
  worker.
- **financials: ~25%.** Exchanges receive 6.5–9.8× the mean, and wrk_company_financials then waits
  4.6 s on 49k balanced rows.

**The broadcast join already existed** (`CompileOptions.BroadcastMaxRows` with `RelationRowCounts`, and
`DeltaRowCounts` to read sizes from Delta-log stats), but nothing set it. `IvmMultiBatch` now takes
`IVM_BCAST_MAXROWS=N`. Interleaved A/B, five runs per arm, all three batches, views identical in all 15:

| | batch-1 drain median | alloc | batches 2 / 3 |
|---|---|---|---|
| no broadcast | 15.59 s | 53.76 GiB | ~0.10 / 0.10 s |
| **≤ 1,000 rows** (trade_type, status_type, industry, tax_rate, audit) | **14.53 s (−6.8%; 5 of 5 pairs)** | 53.74 | unchanged |
| ≤ 30,000 rows (adds hr, date, prospect, customer_mgmt) | 14.83 s (−4.9%) | 55.65 (+1.9) | unchanged |

With broadcast on, total wait falls to 26.0 s. What's left is led by financials' skew (exchanges 484, 494,
496 at 6.5–9.8×) and by fact_trade exchange 245. That exchange's own rows are balanced, so it is waiting
on an upstream operator, the trades Distinct.

Feldera handles the same problem at runtime. ivm-bench's Feldera profile sets `adaptive_joins: true`,
and its `Balancer` (`crates/dbsp/src/operator/dynamic/balance/`) picks shard, broadcast, or balance per
join stream each step from observed key distributions, redistributing the integral when the policy
changes.

**Built (server, default on).** `DbspNetEngine` broadcasts dimensions of at most
`DBSPNET_BROADCAST_MAX_ROWS` rows (default 1,000; `0` or `off` disables) in a parallel deploy
(`BroadcastJoins`). Sizes come from the inputs' Delta-log row counts at deploy. A zero or unreadable
count counts as large, so an empty table, or a fact table staged after deploy, is never broadcast.
ivm-bench stages batch 1 before it starts the engine, so deploy sees the real sizes.

The choice is part of the plan's state layout, so a persistent deploy records `{max_rows, row_counts}`
under `compile.broadcast` in every snapshot's metadata. A restore compiles with the recorded values.
Without them, a dimension grown past the threshold compiles a hash join, and the restore fails with
"snapshot plan fingerprint mismatch". That is a loud failure, not wrong results, but the deploy is
blocked. `DeployResult` reports `BroadcastMaxRows` and `BroadcastInputs`. Tests are in
`BroadcastJoinsTests`; four mutants (ignore the recorded choice, count empty tables as small, drop the
metadata, drop the compile options) each fail them.

## 5. Lessons this pass rests on

- Per-operator wall time in a parallel run follows where GC pauses land. Two IntegratedViews went
  from ~55 ms to ~550 ms in two of three runs with no code change. Use kind totals, allocation, or
  per-tick wall.
- `DBSPNET_STREAM_RELEASE_FIRST_TICK=0` does not recover the 5% since `f02030d`. R0's real cost is
  ~110 ms in tick 0, the reader scan (§5.7).
- The engineered-wood 0.4.0 package (Release, 70 commits past the old submodule) costs ~1% of
  batch 1 against the submodule build.
