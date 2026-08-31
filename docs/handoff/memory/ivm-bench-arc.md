---
name: ivm-bench-arc
description: ACTIVE arc — running the ivm-bench TPC-DI IVM benchmark on DbspNet; gap analysis lives in docs/ivm-bench-gap-analysis.md
metadata: 
  node_type: memory
  type: project
  originSessionId: ccbdc64b-08c3-4107-a2c7-6aaccb97a802
  modified: 2026-07-20T22:47:30.962Z
---

Curt wants DbspNet to participate in [ivm-bench](https://github.com/mdrakiburrahman/ivm-bench)
(TPC-DI-based IVM benchmark, dbt model DAG, Feldera is a participant → our reference
engine class). Started 2026-07-15.

**Full gap analysis is in `docs/ivm-bench-gap-analysis.md`** — that doc is the source of
truth; do not re-derive it.

Two findings worth carrying because they are NOT derivable from the DbspNet repo:

1. **ivm-bench measures full-STATE materialization, not delta emission.** All 16 Feldera
   gold output connectors are `mode: truncate` (full snapshot rewrite per batch),
   `+stored: true` (= `CREATE MATERIALIZED VIEW`), no `__feldera_op`/`__feldera_ts` change
   tags. Write-out is INSIDE the measured window (timer = resume → all sinks drained +
   DBSP commit done). Feldera's output is never validated — the `EXCEPT ALL` check is
   engine-internal and only for duckdb-openivm/spark-openivm. So a delta-emitting engine
   would post a fast number while doing strictly less work, and nothing would catch it.
   Decided: match the measured work (full state + full rewrite).

2. **Feldera DOES support ranking window functions projected into the output** (not
   TopK-restricted). `docs/skipped.md:415-416`'s claim to the contrary is stale — it
   reflects older Feldera docs. Current Feldera docs support them generally with a cost
   warning ("reasonable cost in three circumstances…"). Our O(partition-size)-retraction
   cost analysis is still correct; only the parity justification was wrong. ivm-bench's
   analytics ranks are UNPARTITIONED (whole relation = one partition) = worst case, and it
   demonstrably wedged Feldera at SF=100.

**Order of work (agreed):** SQL surface first, adapters LAST — input/output adapter work is
not on the critical path; the SQL surface determines whether the benchmark runs at all.
Critical-path gaps and status (full detail in the doc):
- Gap 3 (multi-key window ORDER BY): **DONE** (commit 8ae0acb) — offset family only; rank
  family was already multi-key; window aggregates stay single-key by design.
- Gap 4a (computed equi-keys, `CAST(a)=CAST(b)`): **DONE** (commit e885712) — resolver
  lowering hoists key exprs to synthetic columns. Also fixes a latent perf bug: any
  `JOIN ON f(a)=b` was silently a keyless O(n²) cross product.
- Gap 4b (outer join + cross-side residual, AND keyless outer join): **DONE** — plan
  rewrite in PlanToCircuit (`CompileOuterJoinWithResidual`), no operator surgery.
- Gap 5 (STDDEV + VARIANCE family): **DONE** — one invertible SqlStddevAggregator
  (moments n/Σx/Σx²), bare STDDEV=SAMP (matches PG/Spark/DuckDB). Known-dataset test +
  differential PBT cover DISTINCT failure classes (formula vs invertibility) — proven by
  mutation. Gave AVG its first PBT coverage too.
- Gap 2 (nested ROW structs): **DONE by FLATTENING** (not a first-class type). ROW columns
  expand to dotted-name scalar leaf columns at CREATE TABLE; runtime untouched. Curt chose
  the cheap path but asked for a design note: `docs/design-nested-types.md` captures the
  deferred first-class SqlRowType AND why ARRAYS (variable arity) can't be flattened and
  would force it — so arrays are the trigger to build composite types. Limitation: no
  whole-struct SELECT (errors cleanly). Only nested-data model in the benchmark.
- Gap 6 scalar tail: **DONE except named WINDOW.** Shipped MD5 (hex over UTF-8, structural),
  CONCAT_WS (sep-join skip-null, structural), RLIKE (infix → REGEXP_LIKE desugar),
  TINYINT/SMALLINT (→INTEGER, lossy), CAST(bigint AS timestamp) (=MICROSECONDS, DuckDB
  convention; Spark=seconds is a 1-line swap — documented), typed temporal literals
  (DATE/TIME/TIMESTAMP '…' → CAST, mirrors INTERVAL). Suite 1889.
- **STANDUP RAN (2026-07-16)** — measure-first paid off; corrected the static gaps. Harness
  `tests/DbspNet.Tests/Scratch/IvmBenchStandup.cs` (UNCOMMITTED, hardcoded scratchpad path,
  no-ops if absent — keep for measuring progress). Result: **sources 20/20, views 17/50
  compile.** 33 view failures → THREE root causes (~21 are cascades):
  1. **Window fn nested in expression** (SCD2 `is_current`: `CASE WHEN x = MAX(x) OVER(...)`)
     — 5 silver models, huge cascade. #1 blocker. MISSED by static analysis (logged the
     MAX-OVER as supported, missed the enclosing CASE). Fix: lift window agg to hidden col.
  2. **Typeless NULL** — **DONE.** New `SqlNullType` (SQL "unknown"): bare null resolves to
     it; unifies-with-anything in CommonComparableType/CommonNumericType (adopts peer,
     nullable); materializes to typed null in ResolveCast/MaybeCast (both copies —
     Resolver + BuiltinScalarFunctions). Two single-point levers, clean. Standup 17→21
     compile (finwire×3 + crm all clear). Suite 1896.
  3. **Named WINDOW** (2 models) — already-deferred gap-6 item.
- **#1 window-in-expression: DONE.** Resolver collects ALL window nodes from select items
  (not just top-level), lifts each nested aggregate/offset to a hidden column; `preBound`
  identity-substitution rewrites the enclosing expr — no new operator (reused BuildWindowGroup).
  Rank stays TopK-only even nested. `CollectWindowFunctions` walker mirrors
  MentionsWindowFunction. **Standup 21→33 compile.** Suite 1901.
- **NEW roots surfaced (were masked):**
  - **JOIN USING merged key not exposed downstream** — watches (symbol), fact_holdings
    (trade_id). Focused resolver gap: coalesced USING column missing from output schema by
    bare name. Likely next quick-ish fix.
  - financials `ROW_NUMBER() OVER(...) = 1` in CASE — rank-in-expr, correctly rejected (gap 1).
- **Named WINDOW: DONE.** Pure parser substitution — `OVER w` → placeholder WindowSpec{Name},
  parse WINDOW clause after HAVING, SubstituteNamedWindows rewrites items (handles nesting).
  `window` made a RESERVED keyword (else `FROM t WINDOW` misparsed as alias `t AS window`).
  Resolver never sees named windows. Standup 33→34 (daily_market compiles; dependents surfaced
  own roots). Suite pending.
- **SELECT * with a window: DONE.** Star caps at baseSchema.Count in ResolveProjections
  (new starColumnLimit param) so `*` gives base cols only, not hidden window cols.
  dim_customer compiles (34→35). Suite 1908.
- **Remaining roots ranked by cascade (standup 35/50):**
  1. **Type-coercion cluster: DONE.** (a) DATE↔TIMESTAMP coerces DATE→midnight TIMESTAMP
     (default-on, PG/Spark). (b) numeric↔string coercion shipped as an OPT-IN seam
     `NumericStringCoercionMode` (default OFF=PG-faithful; benchmark turns it on). Research
     showed PG is the OUTLIER — SQL Server/Oracle/MySQL/DuckDB/Spark/Calcite all coerce
     string→numeric, incl. all 3 ivm-bench engines. Standup 35→39 (dim_account + fact_trade
     cascade cleared). Curt chose the flag over default-on. Suite pending.
- **JOIN USING: DONE.** Merged key now carries its SOURCE SIDE's qualifier (not null), so
  `s1.*` includes the USING key — PG-faithful; a null qualifier was dropped by qualified-star.
  Root cause: watches_history/holdings_history do `select s1.* … using(symbol/trade_id)`, lost
  the key, downstream (watches/fact_holdings) failed on bare ref. Standup 39→42. Suite 1915.
- **Standup 42/50 — ONE root cause left: GAP 1 rank-in-output.** All 8 remaining failures are
  RANK/ROW_NUMBER/DENSE_RANK selected into output (market_volatility, financials,
  trade_volume_stats, broker_performance, customer_concentration, daily_market_pulse) or a
  cascade from financials (wrk_company_financials, fact_market_history). **The board is clear
  for the gap-1 DECISION** — the sole remaining blocker. Feldera implements general
  rank-in-output (cost = warning not restriction; wedged Feldera at SF=100 on unpartitioned
  analytics ranks). DbspNet restricts to TopK-filter for O(partition-size)-retraction cost.
  Decision: implement it, or run 42/50 partial. Standup harness uncommitted at
  tests/DbspNet.Tests/Scratch/IvmBenchStandup.cs (sets NumericStringCoercionMode.Enabled=true).
- **GAP 1 rank-in-output: DONE + pushed (2026-07-16, commit 3048d09).** Last ivm-bench
  compile blocker closed → all 50 feldera models compile (was 42/50). New `PartitionedRankPlan`
  + `PartitionedRankOp<TInRow,TOutRow,TKey>`, structural path. Forked PartitionedTopKOp's
  sorted trace + rank-assignment and PartitionedWindowAggregateOp's widened diffing; positional
  whole-partition recompute (no value-range). **As-built deviation from the design doc:**
  `_window` keyed by the WIDENED row → weight (not base row) — required so ROW_NUMBER's
  one-base-row→w-distinct-ranks expansion diffs cleanly; borrows TopK's EmitDiff verbatim.
  RANK=1+rowsBefore, DENSE_RANK=distinct-group index (tie groups via ConstantZeroComparer),
  ROW_NUMBER=multiplicity-counted position. Empty PARTITION BY ⇒ one global partition; nested
  financials ROW_NUMBER-in-CASE lifts via preBound (free). No GC (GcFrontier=>null). Resolver:
  rank family in TryResolveWindowAggregate, group by (family, spec, function). Typed/spine punt.
  Correctness: BatchPartitionedRank oracle + differential PBT (8 shapes ×16 seeds, tie-forcing
  domain) + behavioural/snapshot tests. Suite 1939 green. `docs/design-rank-in-output.md` marked
  BUILT; gap-analysis §1 + skipped.md updated.
- Gap 1 (rank-in-output) did NOT surface — masked behind cascade #1 (analytics models fail
  as `unknown table fact_trade`). Still there; reappears once cascade clears.
- **Real remaining critical path = #1 (window-in-expr) + #2 (typeless NULL)**, both resolver
  work, neither previously on the ranked list. Then named WINDOW, then gap 1 decision.
- Gap 1 (rank projected into output): design decision, Feldera does implement it (see above).

- **OUTPUT CONTRACT / stored output: DONE + pushed (2026-07-16, commit f9d67a4).** The
  engine-side piece between the (now complete) SQL surface and the I/O adapters — Curt asked
  whether closing gap 1 left only adapters; answer was no, delta→full-state integration was
  still missing. New `IntegrateOp<TRow>` = DBSP `I` at the output boundary: owns a `ZSetTrace`,
  folds each output delta in (O(|delta|)), passes the delta through unchanged; exposes the full
  view via `IntegratedViewHandle` / `CompiledQuery.CurrentView` / `EnumerateView`. Opt-in
  `CompileOptions.StoredOutput` (default off → delta-only pipeline pays nothing, fingerprint
  unchanged). `ISnapshotable` (view persisted, inside measured commit; snapshot load =
  reset-and-reintegrate `_view.Current`); `GcFrontier=>null` (MV inherently unbounded, by
  design). Reuses `ZSetTrace.Integrate` — no new algebra. Tests: IntegrateOpTests (running-sum/
  multiplicity/drive-to-zero), StoredOutputTests (differential view≡accumulated≡batch across
  filter/join/agg/DISTINCT/rank/UNION ALL), IntegrateSnapshotTests (round-trip). Suite 1954.
  Design: `docs/design-stored-output.md`. **Remaining for an end-to-end run: input adapter
  (Delta/Parquet→InputHandle), output adapter (EnumerateView→Delta truncate write + drain
  signal), real-data correctness pass. All 50 models compile; the full-state output path exists.**

- **DOCKER HARNESS BRING-UP (2026-07-17, ivm-bench branch `dbspnet-engine`).** Running the real
  harness in WSL/Docker on Curt's 24-core/30GB box. Cleared a chain of env/config snags (all
  in ivm-bench, committed to the branch): datagen compose hardcoded 32-CPU / 80g-heap devbox
  sizing → `base_env()` now caps `DATAGEN_CPUS`=min(32,cores) + `DATAGEN_HEAP`=60% host RAM;
  `dbspnet-server` Dockerfile missing `curl` → healthcheck never healthy → cascaded (fixed +
  tightened `_up_with_retry`'s over-broad "is unhealthy"→mssql mislabel). Engine sizing (`DBSPNET_CPUS`)
  auto-sized by resource_calc; datagen is the one unsized path. **Manual repro pattern** (bypasses
  harness teardown so the engine log survives): sudo rm -rf mount/raw/<sf> → datagen (needs
  `BATCH_1/2/3_PCT`) → batch-loader `init` (builds `staging/*` from `batch1/*`) → bring up dbg
  stack → curl /deploy → read `docker logs dbspnet-server`. Staging is built by spark-batch-loader,
  NOT datagen; datagen only writes `batchN/*`.
- **CONNECTOR-SIDE NESTED FLATTENING: DONE + pushed (dbsp-net 898c903, ivm-bench pin d602651).**
  The SQL side already lowered ROW→dotted leaves (gap 2); the missing half was the Delta
  connector — 19/20 flat sources bound, but CustomerMgmt (spark-xml nested Arrow struct) failed
  Bind ("declared column 'customer.account.ca_b_id' has no matching column"). New
  `NestedArrowResolver` (in Connectors.Abstractions): declared name → top-level field OR dotted
  path into nested structs; `Bind` type-checks the LEAF only (never the container struct);
  `ArrowProjection` extracts the leaf per batch **propagating parent-struct nulls** (null ancestor
  ⇒ null leaf, not stale child). Schema-driven not always-flatten (exact top-level match first;
  forward-compatible with future native structs — Curt's design ask). Offset-0 guarded (CDF batches).
  Also: `DbspNet.Server` UseExceptionHandler returns exception detail in the 500 body (opaque /deploy
  500 cost a debugging round-trip). Tests: NestedArrowResolverTests (2-level null propagation);
  connector suite 32 green. **Was the last input-bind blocker — full DAG should now deploy; awaiting
  Curt's Docker re-run.**

- **RUNTIME (batch-execution) fixes over the real historical load (each: self-describing 500 via
  UseExceptionHandler → one-paste diagnosis → fix + round-trip test + pin bump).** After deploy
  succeeded (20 inputs, 16 outputs), running batch 1 surfaced a chain of "bind accepts N physical
  encodings for one SQL type, decode only handled the canonical one": (1) **INT96 timestamps** —
  Delta log says TIMESTAMP but Parquet stores INT96, engineered-wood surfaces raw
  FixedSizeBinary(12) (maps PhysicalType.Int96→FixedSizeBinaryType(12), no conversion; Curt chose
  DbspNet-side workaround over fixing eng-wood); `ArrowColumns.Extract` now decodes INT96
  (int64 nanos-of-day + int32 Julian day) when a TIMESTAMP col is FixedSizeBinary(12). (2)
  **narrow ints** — FromArrowType widens Int8/Int16/Int32→INTEGER but Extract hard-cast Int32Array;
  now dispatches on width. Likely-next of this class (not yet hit): VARCHAR-as-LargeString,
  non-µs timestamp units.
- **THEN OOM in IncrementalLeftJoinOp (24 GB) — DIAGNOSED + FIXED via dead-view elimination
  (commit 0e274be).** Added a per-key cross-product guard (>5M left×right throws with the key) → it
  did NOT fire → aggregate output size, not a degenerate key. IncrementalLeftJoinOp = the
  NO-residual left-join path (residual/SCD joins go through EmitInnerJoin). Traced to
  `fact_market_history`: `daily_market (fact) left join wrk_company_financials USING(sk_company_id)`
  — the right side contributes ZERO output columns and wrk has ~1 row per financial-version, so it's
  a pure ×~20 row-MULTIPLIER over the fact table. AND it's a DEAD leaf (no output binding, only a
  comment references it), computed only because CompileProgram built EVERY view's stream. **Root
  = the row-representation state-size face** (fat object[] rows can't materialise a large view where
  Feldera's compact rows do) but this specific wall was largely AVOIDABLE: `CompileProgram` now
  prunes views not reachable from an output (backward pass over topo-ordered views via CollectScans).
  fact_market_history + daily_market_pulse (both +stored-but-unwritten leaves) no longer computed.
  If a REAL output OOMs next → THAT's the genuine columnar/SF-1 conversation. **Parity nuance:
  Feldera +stores these 2 views (maintains state); DbspNet now skips them — measured 16 outputs
  identical, but DbspNet does less memory work on those 2.** Guard is a permanent safety net.
- **Docker-run env fixes (ivm-bench branch, all in `base_env()` / composes):** DATAGEN_CPUS cap,
  DATAGEN_HEAP=60%-host-RAM, dbspnet Dockerfile curl for healthcheck, `_up_with_retry` mssql-mislabel
  tighten, manual-repro needs BATCH_1/2/3_PCT + sudo rm root-owned mount/raw. Manual-repro pattern
  (bypasses harness teardown so the engine log survives): datagen → batch-loader `init` (builds
  staging/* from batch1/*) → dbg stack up → deploy(:5000) → resume/wait(:8081 direct, self-describing).

- **FIRST SF=3 END-TO-END RUN (2026-07-17): batch 1 executes the full 16-output DAG in ~112s**,
  but 8 outputs were EMPTY (dim_account + all fact tables that INNER-join it + their analytics),
  cascading from dim_account=0. **ROOT CAUSE (after a long hunt): a JOIN-KEY COERCION bug, NOT
  the nested read / engineered-wood.** dim_account does `accounts JOIN dim_broker USING(broker_id)`
  where accounts.broker_id is BIGINT (ca_b_id) and dim_broker.broker_id is VARCHAR (employee_id).
  Under numeric<->string coercion the values are equal (manual join = 11055 matches), but equi/USING
  joins built keys from the RAW column types → a BIGINT key row and a VARCHAR key row never hash-equal
  → 0 rows. **FIX (dbsp-net 2d2d942):** `ResolveUsingJoin` casts each mismatched key column to the
  common type in place (`CoerceKeyColumns`; merged output col already declares keyType); `TryExtractEquiKey`
  defers mismatched bare ON-keys to the computed-key path whose `HoistKeySide` now casts via `MaybeCast`.
  Full suite 2004 green. Test `DimAccountJoinShapeTests`. (Typed single-query path throws on hoisted-cast
  ON-keys — latent, benchmark uses structural CompileProgram, no test hits it; follow-on.)
- **engineered-wood was a RED HERRING — do not re-investigate it for this.** Extensive local repro
  (copy the Delta tables out of WSL to D:/src, drive `DeltaInputConnector`/`ParquetFileReader` in a
  gated test) PROVED the connector reads the nested CustomerMgmt data correctly on the ORIGINAL
  engineered-wood: Customer.Account._CA_ID = 12860 non-null, ca_b_id 1–14994, _ActionTS = sane 2007
  dates, _C_ID present on all account rows. The dim_customer temporal join also matched all 12860.
  Only the broker USING join dropped everything. Tested contributor PR cmettler/engineered-wood#4
  (13fead6, "Preserving downstream fixes") by repointing the submodule → did NOT fix it (its null-struct
  alignment fix is WRITER-side, ours is a Spark-written READ; PR untouched `NestedAssembler`). Reverted
  the submodule to CurtHagenlocher/engineered-wood@09ce9c1. (PR#4 does carry a Decimal128-reader fix
  overlapping our own DbspNet-side Decimal64 decode, if ever wanted.)
- **The earlier RUNTIME decode chain (all real fixes, shipped): INT96 timestamps, narrow ints
  (Int8/16→INTEGER), narrow decimals (Decimal32/64→DECIMAL)** — all "Delta log says type X, Parquet
  physical is narrower/legacy" mismatches in ArrowColumns.Extract; each with a round-trip test.
- **STATUS: batch-1 correctness fix pushed; awaiting Curt's re-run.** Expectation: dim_account≈11055
  and the 8 zero outputs populate. Then: full 3-batch harness vs Feldera = the actual benchmark result.
- **JOIN RESIDUAL PUSHDOWN: DONE + pushed (dbsp-net 357dd6f, ivm-bench pin 709ecab).** The
  memory-tight SF=3 wall (temporal SCD joins `key AND ts BETWEEN lo AND hi` built the full per-key
  A×B equi product then post-filtered → OOM ~24-25g). Fix = wire the residual into the STRUCTURAL
  join combine (operator-level `Func<TOut,bool>? residual` in JoinInto already existed from d93db1d
  for the typed path; PlanToCircuit still post-filtered). INNER + LEFT/RIGHT/FULL covered; spine
  keeps post-filter fallback (benchmark is structural). Result set identical, intermediate shrinks.
  See [[residual-pushdown-next]] for as-built + the Sql.dll flaky-incremental-build landmine.
  Awaiting Curt's SF=3 re-run: expect no OOM, then full 3-batch harness vs Feldera.

- **OUTPUT VALIDATION (2026-07-17): [SUPERSEDED — the writer block was RESOLVED; see below].** The
  writer-readability gap here was fixed by the `OmitPathInSchema=false` override
  (DeltaOutputConnector.cs:75-87, [[engineered-wood-path-in-schema]]); the SF=3 DbspNet-vs-Feldera
  comparison subsequently RAN and produced findings ([[ivm-bench-validation-findings]] — 9 views
  correct + real engine bugs since fixed). So `PRESERVE_RESULTS=1` + `compare_outputs.py` value-diff is
  NOT blocked; only a narrower engineered-wood snappy READ bug remains on some table(s). Original
  (stale) note preserved below for context. ivm-bench never validates Feldera/DbspNet output (only `*-openivm` get the
  EXCEPT-ALL-vs-Spark check). Built the machinery to diff DbspNet vs Feldera: `PRESERVE_RESULTS=1`
  seam (oat_runner + benchmark-server compose) keeps both engines' `mount/results/<sf>/<engine>/gold/`
  Delta tables past cleanup; `src/.scripts/compare_outputs.py` set-diffs the 16 gold views (EXCEPT ALL
  both ways, exact_diff + data_diff-excl-sk_*, float-rounded). BUT **both DuckDB `delta_scan` (rejects
  `metaData.format.options` unmasked-nulls) AND DuckDB/pyarrow `read_parquet` (TProtocolException on the
  footer) can't read engineered-wood's WRITTEN parquet** — its writer footer is non-spec (exactly the
  cmettler PR#4 writer fixes: all-null-page PLAIN, checkpoint/format schema). engineered-wood reads its
  OWN + Spark output fine; only its writer output is unreadable externally. **Fix = adopt cmettler's
  writer commits (standard-readable output, also makes DbspNet Delta output consumable downstream) →
  compare_outputs.py then works. A clean fresh-session task.** Fallbacks: .NET diff via engineered-wood
  DeltaTable (reads both, needs copying the results dirs); or incremental≡batch self-consistency.
  CONFIDENCE meanwhile: dim_account=11055 matched an INDEPENDENT DuckDB join over real inputs; counts
  sensible; incremental≡batch PBT-proven internally.

**Method that keeps paying off:** mutation testing catches vacuous tests. Found weak tests
in every gap so far — a structural-path test that never ran (typed path tried first), an
ASC tie-break that couldn't detect a dropped key, a left-only residual that didn't exercise
the remap, and a NULL-safety shape the residual couldn't reach. Also: build the PBT/oracle
BEFORE the feature (4b), so the test that judges the rewrite exists before the rewrite —
and fix the batch oracle first (it's the independent reference).

- **FIRST FULL HEAD-TO-HEAD RESULT (2026-07-17): DbspNet BEATS Feldera on all 3 batches at SF=3.**
  Full `benchmark.sh` (both engines, 3 batches, serial) completed in ~11.5min. Per-batch wall:
  **b1 (100% load) DbspNet 2:25 vs Feldera 5:17 (~2.2x); b2 (1% incr) 3s vs 22s (~7x); b3 (1% incr)
  2s vs 19s (~9.5x).** The incremental batches (IVM's whole point) are the decisive win — 7–9x.
  Validated for the first time: both engines side-by-side + the batch-loader→resume/wait INCREMENTAL
  path (b2/b3). RESULTS.md's openivm/spark ratio + break-even tables are N/A (different engine pair).
- **SF=3 MEMORY: needs ~28GB working set** (all 16 full-state materialized views + join intermediates;
  full-state output is inherent to ivm-bench's measured work). Harness auto-sizes engine to host−dbt
  (~25GB on a 30GB WSL VM → OOMs). **Ran by raising the WSL VM to 48GB** (host 64GB; `.wslconfig
  [wsl2] memory=48GB` → engine ~40GB). Residual pushdown ([[residual-pushdown-next]]) bought ~2GB on
  the JOIN phase but the IntegrateOp (output-state) footprint is the ceiling → **row-rep/columnar work
  is the real lever for SF=100** (see row-representation memory notes). SF=3 works at 40GB engine mem.

Related: [[dbspnet-overview]], [[feldera-comparison-benchmarks]], [[roadmap-candidates]],
[[residual-pushdown-next]], [[partitioned-topk]], [[window-aggregates]]
