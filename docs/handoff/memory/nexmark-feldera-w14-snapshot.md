---
name: nexmark-feldera-w14-snapshot
description: Nexmark DbspNet-vs-Feldera comparison runs (2026-06-09 → Run E 2026-06-11 post-§22.7). LATEST (Run E): §22.7 q19 narrow-key default-on CONFIRMED vs Feldera (10c 0.52→1.09× LEAD, 14c 0.62→0.96×, 1c +34%); q18 remains worst (0.38-0.64×, §22.8 NO-BUILD, no cheap lever); q4 held §21 gain (1.13× 14c); q2 de-scales past 10c across 3 runs
metadata: 
  node_type: memory
  type: project
  originSessionId: a4b020b6-5826-4371-a771-fe45abd2c5bd
---

Point-in-time Nexmark throughput comparison, **measured 2026-06-09** by the user
on a non-Windows box (Feldera won't build under Windows) — **the box is an Apple
M4 Pro, a HYBRID 14-core part (~10 P + 4 E), confirmed 2026-06-10**; both DbspNet
and Feldera numbers come from it. (So "14 cores" includes 4 efficiency cores — see
[[exchange-scaling-decomposition]] §15.3: running DbspNet at W=14 puts 4 workers
on permanent-straggler E-cores, and W≈10 may scale better.) Numbers are ephemeral
(not in the repo) — persisted here as the launch point for the row-representation
design session. See [[feldera-comparison-benchmarks]] (harness), [[parallel-pipeline-perf]]
(perf work + diagnosis), [[row-representation-design]] (the design doc + prior art).

## The numbers (events/s; Feldera at 14 cores)

### Run E — 2026-06-11, AFTER §22.7 narrow-key partitioned TOP-K DEFAULT-ON (the [[partitioned-topk-row-narrowing]] q19 win). `D:\src\tmp\dbsp-bench-4.txt` (14c + 10c + 1c)

**§22.7 q19 win CONFIRMED vs Feldera — the external validation the in-repo same-box A/B couldn't give.** q19 single-core 696K→**935K/s (+34%)** (a genuine per-row improvement from the narrow `{order,wideRow}` key), and it carried through scaling exactly like the q4 §21 win two runs prior: **14c 0.62→0.96×** (parity), **10c 0.52→1.09× (crossed into the LEAD)**. 1c ratio still 0.43× (Feldera's native engine leads single-core, as always). The narrow-key lever delivered competitively, not just same-box.

**q18 remains the clear worst — consistent with §22.8's NO-BUILD.** 1c **0.38×**, 10c **0.40×**, 14c **0.64×** (improved 0.47→0.64× at 14c via better parallel SCALING only — W=14 1.79M→2.49M — NOT a narrowing lever; §22.7 leaves q18 TOP-1 on whole-row). q18 scales poorly (10c only 1.69M = ~1.8× on 10 cores; many tiny (bidder,auction) partitions). §22.8 measured shuffle-narrowing → NO-BUILD (two narrowable halves never coexist; op floor untouched). q18's gap = per-row op floor + SELECT * wide-row materialization, no cheap lever.

| Query | 1c | 10c | 14c | note |
|---|---|---|---|---|
| q18 | **0.38×** | **0.40×** | 0.64× | worst; §22.8 NO-BUILD |
| q19 | 0.43× | **1.09×** | 0.96× | §22.7 WIN confirmed (was 0.52/0.62) |
| q15 | 0.39× | 1.21× | 1.82× | few-days cap (1c bad, scales) |
| q4  | 0.44× | 1.05× | 1.13× | held §21 gain |
| q22 | 0.43× | 0.67× | 0.96× | string boundary |
| q0  | 0.46× | 0.88× | 0.85× | passthrough boundary |
| q2  | 0.58× | 0.90× | 0.82× | de-scales past 10c (3 runs) |
| q9 1.08/1.59/1.54× · q16 0.86/2.91/6.19× · q17 0.81/2.38/3.37× · q3 3.02/2.01/2.09× · q20 0.71/1.06/1.31× |

**DbspNet now leads/ties 9/13 at 14c** (q1,q3,q4,q9,q15,q16,q17,q20,q22); q18/q19 closing. **q2 negative scaling past 10c CONFIRMED across 3 runs** (14c 7.57M < 10c 9.95M; sweet spot 10c 0.90×) — the one reproducible DbspNet de-scaling bottleneck, a candidate q2 exchange/key-distribution profile if pursued (but exchange/scaling, which §15 deprioritized vs per-row). Note explicitly: "per-row optimizations like q4/q19 remain the highest-leverage work, since they lift all three core counts at once."

### Run D — 2026-06-11, AFTER §21 join column pruning DEFAULT-ON (the [[join-column-pruning]] win); §18 still OFF. `D:\src\tmp\dbsp-bench-3.txt` (14c + 10c + 1c)

**q4 is fixed — the §21 win, and it's per-row not just scaling.** Single-core q4
856K→1.56M/s (+82%, ratio **0.21→0.42×**); 14c **0.66→1.05× (now AHEAD of Feldera)**;
10c 0.97× (parity). That's base-throughput that multiplies through scaling. (§18 is
still default-off — productizing it would push q4 further; lower priority now it's
competitive.) Also q22 14c 0.72→0.98×, q17 14c 3.96×.

**The ranking reshaped — q18/q19 are now the clear worst at every core count:**

| Query | 1c ratio | 10c ratio | 14c ratio | note |
|---|---|---|---|---|
| q19 | **0.32×** | 0.52× | 0.62× | TOP-10 per auction |
| q18 | **0.34×** | **0.46×** | 0.47× | dedup latest bid (TOP-1) |
| q15 | 0.39× | 1.09× | 1.66× | few-days cap (1c bad, scales) |
| q22 | 0.39× | 0.79× | 0.98× | string SPLIT (boundary) |
| q4  | 0.42× | 0.97× | 1.05× | FIXED by §21 |
| q0  | 0.51× | 0.95× | 0.96× | passthrough (boundary) |
| q2  | 0.56× | 0.89× | **0.75×** | NEGATIVE scaling past 10c |
| q20 | 0.72× | 0.96× | 1.26× | |
| q9 1.01× / q16 0.89× / q3 3.19× (1c). DbspNet wins q1,q3,q9,q15,q16,q17,q20 competitively. |

**q18/q19 confirmed NOT column-prunable** — both `SELECT auction,bidder,price,channel,
url,date_time,extra` = ALL 7 bid columns (effectively `SELECT *`), so §21's join lever
does NOT transfer (§19 was right: "needs full rows for output"). Their gap = the
boundary (object[] ingest + wide output) + TOP-K state + (multi-core) out-of-`Step`
output decode. This IS the genuinely-wide residual §21.6 scoped columnar to.

**q2 negative scaling past ~10 cores CONFIRMED (not noise):** 14c 7.02M < its own 10c
9.79M. Only DbspNet query that de-scales past 10 workers — a specific exchange/merge
contention on its key distribution; targeted profile if q2 matters.

**Next lever (recommended): typed ingest** (§17.4 #4 / [[per-row-execution-efficiency]]
§16.9 did the OUTPUT boundary half; this is the symmetric INGEST half — source emits
typed rows, no object[] at the scan). Low-risk, pre-scoped, and hits the whole
boundary-bound laggard cluster q0/q2/q22/q18/q19 at once, not one query. Columnar TOP-K
state stays the deferred residual for q18/q19 if typed ingest doesn't close them (§19
cautions: a TOP-K container change regressed q9).

### Run B — 2026-06-10, AFTER the flat lazy merge-view (commit 3df999e), before the q15/16/17 FILTER housekeeping

| Query | DbspNet W=1 | DbspNet W=14 | Feldera 14c | W14/Feldera | (prev, Run A) |
|---|---|---|---|---|---|
| q0  | 2.58M | 8.24M | 9.09M | 0.91× | (0.90×) |
| q1  | 2.09M | 8.82M | 8.03M | 1.10× | (0.98×) |
| q2  | 3.82M | 11.73M | 9.52M | 1.23× | (0.98×) |
| q3  | 18.80M | 14.29M | 6.31M | 2.27× | (2.12×) |
| q4  | 0.78M | 2.25M | 3.43M | **0.66×** | (0.53×) |
| q9  | 1.14M | 3.45M | 2.10M | **1.64×** | (1.47×) |
| q15 | 0.81M | 0.84M | 0.50M | **1.69×** | (0.27×) |
| q16 | 0.72M | 1.88M | 0.33M | **5.70×** | (0.99×) |
| q17 | 0.95M | 2.65M | 0.93M | 2.85× | (3.20×) |
| q18 | 0.74M | 1.57M | 3.87M | 0.41× | (0.44×) |
| q19 | 0.57M | 2.38M | 3.54M | 0.67× | (0.73×) |
| q20 | 1.50M | 3.96M | 3.81M | 1.04× | (1.07×) |
| q22 | 1.42M | 6.01M | 8.62M | 0.70× | (0.84×) |

**The lazy merge-view's fingerprint (Run B vs A): wins land on aggregate-heavy,
LARGE-GROUP queries, scaling with group size K** — the O(K²)→O(K) prediction
confirmed in the real comparison. q15 (GROUP BY day → few ENORMOUS groups)
0.27→1.69× (6.3×; W=1 alone 0.18→0.81M = 4.5×, a single-thread per-group-cost
win, NOT parallelism — q15 scaling stays ~1×, the few-days cap, but per-group
cost dropped enough to beat Feldera anyway); q16 (channel,day; large) 0.99→5.70×;
q4 (per-auction MAX) 0.53→0.66× (matches the q4flat ~1.25× query-level
prediction); q9 1.47→1.64×. **q17 (GROUP BY auction,day → MANY SMALL groups, small
K) barely moves / slight dip 3.20→2.85×** — the control: little O(K²) to remove.
Other deltas (q18 0.44→0.41, q19, q20, q22 0.84→0.70) are non-aggregate
(TOP-K/string/join) → run/machine variance, not the lazy view.

**Competitive picture now:** DbspNet wins/ties q1,q2,q3,q9,q15,q16,q17,q20, ~parity
q0. Remaining gaps q4 (0.66, improving), q18 (0.41), q19 (0.67), q22 (0.70) are ALL
wide-row exchange/scaling-bound (q18 profiled → exchange ceiling, [[surrogate-key-design]]).

### Run C — 2026-06-10, W=10 (P-cores only) vs W=14, both engines (the §15.8 experiment)

Ran to test [[exchange-scaling-decomposition]] lever 1 (cap W at the M4 Pro's 10
P-cores). **Head-to-head at 10c (DbspNet W=10 / Feldera 10c):** q3 1.87×, q16
3.25×, q17 2.47×, q9 1.48×, q15 1.17×, q2 1.05× (win/tie); q20 0.89×, q1 0.87×, q0
0.86×, q22 0.68×, q19 0.55×, **q4 0.49×**, q18 0.46× (gaps). **Key same-engine
14→10 finding: Feldera is UNIFORMLY faster at 10 (every query +2–38%) — classic
synchronous-BSP straggler, E-cores are pure drag; DbspNet is W-INSENSITIVE (mixed
±7–22%).** So dropping to 10c moves ratios TOWARD Feldera (q4 0.66→0.49×) — lever 1
FALSIFIED as a competitive move (see [[exchange-scaling-decomposition]] §15.8: gap
is per-row efficiency, not scaling; Feldera/DBSP is synchronous BSP not async).

### Run A — 2026-06-09, pre-lazy-view baseline (kept for the before/after)

| Query | DbspNet W=1 | DbspNet W=14 | Feldera 14c | W14/Feldera | W1→W14 scaling |
|---|---|---|---|---|---|
| q0  | 2.65M | 8.28M | 9.23M | 0.90× | 3.1× |
| q1  | 2.09M | 8.10M | 8.24M | 0.98× | 3.9× |
| q2  | 3.99M | 9.38M | 9.59M | 0.98× | 2.4× |
| q3  | 18.4M | 13.1M | 6.21M | 2.12× | 0.71× (neg) |
| q4  | 0.66M | 1.88M | 3.54M | 0.53× | 2.8× |
| q9  | 1.11M | 3.16M | 2.14M | 1.47× | 2.8× |
| q15 | 0.18M | 0.13M | 0.50M | 0.27× | 0.75× (neg) |
| q16 | 0.13M | 0.32M | 0.33M | 0.99× | 2.6× |
| q17 | 0.95M | 3.08M | 0.96M | 3.20× | 3.3× |
| q18 | 0.60M | 1.66M | 3.80M | 0.44× | 2.8× |
| q19 | 0.47M | 2.28M | 3.11M | 0.73× | 4.9× |
| q20 | 1.53M | 3.80M | 3.54M | 1.07× | 2.5× |
| q22 | 1.59M | 7.34M | 8.69M | 0.84× | 4.6× |

(Run B's q15/16/17 use the OLD SUM(CASE) SQL; the FILTER rewrite (d4550d5) landed
after and is equivalent — a fresh run with it pending. The next comparison run
supersedes both tables.)

## What this run validated (the recent work)

- **q17 single-only → 3.20× over Feldera** (single-thread ≈ Feldera's 14-core!).
- **q16 single-only → 0.99×** (parity). **q22 0.22× → 0.84×** (typed SPLIT).
  All from the parallel-aggregate-for-expression-keys + typed-SPLIT + dedup fixes
  (see [[parallel-pipeline-perf]] q15/q16/q17 + q22 entries).
- The ±deltas on q4/q18/q19/q20 vs the prior run are mostly machine/run variance
  (different box) — don't over-read them.

## THE conclusion → next lever

**Parallel-scaling efficiency is the ceiling: every query scales only ~2.4–4.9×
on 14 cores (~20–35% efficiency).** Almost every remaining competitive gap is a
*scaling* gap, not a per-row-logic gap — q0/q22/q19 would flip to wins with even
modestly better scaling; q4/q18 would close a lot. Two tells that the bottleneck
is the **input/exchange layer (whole-row hashing to shard)**, not the operators:
(1) q2 (cheap filter, outputs ~1/123) scales *worse* (2.4×) than q0 (passthrough,
3.1×) → cost is distributing rows, not processing them; (2) q17 (heavy aggregation)
scales *better* than both. This is exactly the row-representation diagnosis
(Z-sets are `Dictionary<TRow,…>`; exchange/join/aggregate hash full row structs).

**UPDATE 2026-06-10 — the row-rep flat-path arc is DONE; the lever has moved.**
The "row representation" design session this section launched RAN: surrogate keys
were measured DOMINATED and the **flat lazy merge-view** shipped instead
([[surrogate-key-design]]), which Run B above shows closed the aggregate-heavy
gaps (q4/q15/q16/q9). The remaining gaps (q4 residual, q18, q19, q22) are now all
the **exchange / parallel-scaling-efficiency ceiling for wide rows** — q18 was
profiled (step-bound, step saturates ~W=12 at ~3×, exchange not op). So the
current **#1 lever = the exchange / parallel-scaling work** (a design-first
session of its own): the all-to-all shuffle of wide rows + per-tick rebuild +
barrier coordination that caps every query at ~2.4–4.9× on 14c. The original
candidates below are the historical framing that led to the lazy view.

**(historical) #1 lever = row representation** (design-first, its own session). Open candidates
(Option 1 spine merge-probe already tried, lost to flat): **integer surrogate keys /
dictionary-encoded rows** (hash ints not structs — Feldera/DBSP does this; biggest
ROI) and **shared arrangements**. Plan: design doc first, prototype only the top
option behind a benchmark. **Use Opus for the design + first prototype** (highest
capability-demand, cross-cutting change touching Z-set repr/exchanges/joins/
aggregates/codecs/persistence); a lighter model (Fable) is a reasonable trade only
for the later mechanical implementation grind once the design is locked.

## Inherently-limited queries (NOT worth chasing)

- **q15 (0.27×):** groups by `CAST(date_time AS DATE)` ALONE → few distinct days →
  almost no groups to parallelize (0.75× scaling) AND 8 large exact `COUNT(DISTINCT)`
  per group. Loses on both axes; group-parallel model can't help. Only cheap lever:
  a no-boxing typed `COUNT(DISTINCT)` for int/long args (drops the
  `Dictionary<object,…>` boxing) would help its single-thread cost but won't beat
  the few-groups cap.
- **q3 (neg scaling):** tiny filtered join; exchange/coordination > parallel gain;
  inherent, already documented. Beats Feldera at every W anyway.

## q18 PROFILED (2026-06-10, commit 2e5911e — `dotnet run -- q18profile`, docs/q18-profile.md)

Answered the deferred question. q18 (dedup latest bid per (bidder,auction), TOP-1)
is **STEP-bound, NOT egest-bound**: W=1 step ~1.6s vs split ~0.45s, gather ~5ms;
output only **9,200 rows** (dedup collapses heavily → ~9.2k live partitions each
accumulating ~100 bids, NOT size-1 as first theorized). **Step scales only
~2.4–3× and saturates by ~W=12** (W=12 and W=24 within noise of each other). The
TOP-K op is already O(1)-window (`ComputeWindow` takes the first sorted row for
limit=1) with a tiny per-partition `SortedDictionary` and negligible gather — so
there is **NO cheap q18-specific operator fix**. q18's gap is the **wide-row
inter-worker exchange / coordination ceiling inside step** — the same root cause
as the overall W14 scaling diagnosis (the #1 lever = exchange/parallel-scaling
for wide rows). Profiling method (`Q18ProfileBenchmark`): W-sweep via
`SpineParallelHarness`, split/step/gather decomposition, reusable for other
queries.

## Also pending (cheap, between-runs) — q15/16/17 FILTER rewrite DONE

DONE 2026-06-10 (commit d4550d5): q15/q16/q17 re-authored to verbatim Feldera
`COUNT(*) FILTER (WHERE p)` / `COUNT(DISTINCT x) FILTER (WHERE p)` form. FILTER is
parser sugar lowering to `COUNT(CASE WHEN p THEN …)`, so results identical to the
prior `SUM(CASE 1/0)` form; only plan change is SUM→COUNT on the plain counts.
Verified via `nexmark`: all W>1≡W1 cross-checks pass, q15/16/17 still parallelize
(q15 1.15× few-days cap, q16 2.13×, q17 4.29×) — no single-only regression.
