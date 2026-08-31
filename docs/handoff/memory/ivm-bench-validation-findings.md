---
name: ivm-bench-validation-findings
description: "TPC-DI DbspNet-vs-Feldera correctness comparison (SF=3): 9 views correct + 4 engine bugs + 1 eng-wood read bug. FIXED: #1 (CAST(DECIMAL AS DOUBLE) fraction drop), #4 (default NULL collation High→Low seam for Feldera). #2 (market_volatility) = RESOLVED, DbspNet CORRECT (1 row/symbol), Feldera OVER-PRODUCES ×3/×5 dup rows from unconsolidated global CROSS-JOIN aggregate → Feldera quirk not DbspNet bug. #3 (SCD-2 same-timestamp ties) = DbspNet VERIFIED spec-correct, Feldera peer-LAG quirk → caveat. #5 (snappy read) = FIXED by cmettler fork (dim_customer reads); TODO cherry-pick to original + revert fork. NET: no substantive DbspNet correctness bugs remain — residuals are cosmetic (dim_customer phone null/empty) + minor (4th-decimal ROUND) + #3 caveat"
metadata:
  type: project
  originSessionId: d04e1240-137c-49b7-b437-e6290184b5c5
---

**CONTEXT:** After the [[engineered-wood-path-in-schema]] fix made DbspNet's output readable,
the SF=3 DbspNet-vs-Feldera correctness comparison RAN. Tools: `src/.scripts/compare_outputs.py`
(row-level EXCEPT-ALL per view, excludes sk_*) + `src/.scripts/diff_columns.py` (per-column value-
multiset diff + sample divergent values; committed on ivm-bench `dbspnet-engine`). Key gotcha:
`exact_diff == 2×rowcount` with `data_diff == 0` = surrogate-key (`sk_*` = md5, timestamp-format-
sensitive) formatting only, NOT a real diff. The gold view SQL is per-engine dbt projects at
`src/containers/dbt-server/dbt-projects/<engine>/models/gold/...`; **dbspnet's SQL is IDENTICAL to
feldera's** for the flagged views (verified by diff) → the diffs are genuine DbspNet ENGINE bugs,
not SQL-dialect differences.

**CORRECT (9 views):** dim_broker, dim_company, dim_date, dim_security, fact_cash_balances,
fact_cash_transactions, fact_holdings, fact_trade (all sk-only), AND customer_concentration
(every metric column matches per-column).

**REAL DbspNet BUGS (priority order):**
1. **DECIMAL(38,4) aggregation — FIXED (dbsp-net 6e5236a, pushed 2026-07-17).** Root cause was NOT
   SUM or COUNT (both correct) but `CAST(DECIMAL AS DOUBLE)`: the real avg_* expr is
   `CAST(SUM(CAST(ROUND(x,4) AS DECIMAL(38,4))) AS DOUBLE) / NULLIF(COUNT(x),0)`. `BuildCastFromDecimal`
   rescaled the mantissa to scale 0 before `(double)` — correct for INT/BIGINT, but for FLOAT/DOUBLE it
   dropped every fractional digit (CAST(60.7834 AS DOUBLE) → 61.0). That's why all avg_* (DECIMAL→DOUBLE)
   diverged and all pure-DOUBLE cols matched — the EXACT correlation observed. Fix: new
   `DecimalRuntime.ToDouble` (mantissa/10^scale) routed for float/double in BOTH ExpressionCompiler and
   TypedExpressionCompiler; int/long keep truncating. Regression suite `DecimalToDoubleCastTests`
   (structural + typed + int-unchanged + the ivm-bench avg shape). Full suite 2010 green. The trivial
   `SUM+COUNT` repro from the old note did NOT reproduce — the CAST(...AS DOUBLE) wrapper is essential.
   Should clear avg_* in both trade_volume_stats + broker_performance; re-run to confirm.
2. **market_volatility drops ~2/3 rows (dbspnet 1796 vs feldera 5550).** Same SQL. dbspnet rows are a
   strict SUBSET (missing symbols, not miscomputing). **INVESTIGATED 2026-07-17 → LAG/COUNT hypothesis
   FALSIFIED; ENGINE QUERY PATH VERIFIED CORRECT.** Reproduced the full query shape against DbspNet.Sql
   (same compiler the harness drives) and every part preserves rows/counts: LAG/LEAD partitioned offset
   (batch + incremental one-step-per-day + CDC retraction/update); COUNT(col)/COUNT(DISTINCT)/HAVING>=3;
   full market_volatility (all aggs + global_market ungrouped agg + CROSS JOIN); **unpartitioned
   RANK() OVER(ORDER BY) in scored output (all rows kept)**; and upstream silver daily_market window
   funcs (running MIN/MAX OVER + MAX over mostly-NULL date flag). Could NOT drop a single row via query
   logic. Symptom (strict subset + present symbols' metrics exactly correct) ⇒ whole symbols missing ⇒
   the aggregation engine. **RESOLVED 2026-07-18 → NOT A DbspNet BUG; DbspNet is CORRECT, Feldera
   OVER-PRODUCES (like #3). The "strict subset / missing symbols" read was WRONG.** Probed the PRESERVED
   outputs: dbspnet = 1796 symbols × EXACTLY 1 row each (correct for GROUP BY dm_s_symb); feldera = SAME
   1796 symbols but 357×1 + 1001×3 + 438×5 rows = 5550 (DUPLICATES, 0 symbols missing from dbspnet). For a
   sample symbol Feldera's 3 copies are byte-identical EXCEPT `volatility_z_score` (-1.0845 ×2 vs -1.0846
   ×1) — the one column derived from `global_market` (the ungrouped CROSS-JOINed aggregate). So Feldera
   holds multiple UNCONSOLIDATED versions of the global aggregate (slightly different mkt_avg/std_volatility
   across batches) and the CROSS JOIN multiplies every symbol ×3/×5; DbspNet consolidates to the 1 correct
   row. DbspNet's per-symbol aggregates all match. Only DbspNet-side nuance = the z-score last digit
   (-1.0846) = same 4th-decimal ROUND residual. So #2 = Feldera duplicate-row artifact, DbspNet correct.
   (Probes: probe_mv.py / probe_mv2.py, /tmp on Curt's WSL.)
3. **SCD-2 same-timestamp ties — DbspNet VERIFIED CORRECT; #3 is a FELDERA quirk, not a DbspNet bug
   (investigated 2026-07-17).** dim_trade +12 = 6 trades × 2 extra rows; each divergent trade has ALL
   its history versions sharing ONE effective_timestamp (=th_dts) — genuine same-timestamp SCD. Per
   trade DbspNet emits 3 rows (1 is_current=true + 2 false); Feldera emits 5 (3 true + 2 false). The
   `trades_history` shape is `is_current = LAG(t_id) OVER(PARTITION BY t_id ORDER BY th_dts DESC,
   t_dts DESC, t_st_id DESC, th_st_id DESC) IS NULL`. Repro'd the LAG shape directly against DbspNet.Sql
   (throwaway ZzScdLagRepro, since deleted): DbspNet (a) PRESERVES bag multiplicity for byte-identical
   rows (3 dups → 3 output rows, 1 LAG=NULL + 2 LAG=prev) and (b) yields EXACTLY ONE is_current=true per
   partition for K identical/tied rows — both STANDARD-SQL-CORRECT. Feldera's 3-trues-per-trade is a
   PEER/RANGE-style LAG over tied rows (every row tied at the front gets LAG=NULL), non-standard for
   row-offset LAG/LEAD. So DbspNet is the MORE-correct engine; resolution = documented benchmark caveat,
   NOT a code change. (Residual: Feldera's exact source row count 3-vs-5 unprovable — mount/raw is always
   wiped; would need a datagen re-run — but doesn't change the verdict.) fact_watches +12 likely the same
   same-timestamp-tie family. dim_account's 4 (effective_timestamp only) likely same class. Optional:
   promote the ZzScdLagRepro cases into LagLeadTests as permanent bag-multiplicity/tie regression tests.
4. **broker_performance RANK ties — FIXED (dbsp-net a279f7e, pushed 2026-07-17).** Core tie logic was
   VERIFIED CORRECT (RANK ties 1,1,3,4,4; DENSE_RANK 1,1,2,3,3). Root cause = **default NULL collation**:
   Curt confirmed Feldera source `NULL_COLLATION = NullCollation.LOW` (nulls smallest → last under DESC),
   but DbspNet was hardwired to `HIGH` (Postgres: `_ => descending` at 3 Resolver ORDER-BY sites) → a
   bare `DENSE_RANK() OVER (ORDER BY total_commission DESC)` put an all-NULL group at rank #1 and shifted
   every rank below. Fix mirrors the [[numeric-string-coercion]]/NumericStringCoercionMode seam: new
   `NullCollationMode` (ThreadStatic) + public `NullCollation {High,Low,First,Last}` enum (Calcite mirror);
   `NullCollationMode.DefaultNullsFirst(descending)` replaces the 3 `_ => descending` defaults; threaded
   through `SqlProgram.Compile/Resolve` as opt-in `nullCollation` (default High = unchanged, suite 2013
   green). **DbspNetEngine (the Feldera-compat front-end) now passes `nullCollation: NullCollation.Low`**
   at both compile sites (same place it hardcodes numericStringCoercion:true). `NullCollationTests` cover
   Low/High/default. Re-run to confirm broker_performance ranks clear.

**SEPARATE (engineered-wood, not a data diff):**
5. **dim_customer "Corrupt snappy compressed data"** — data-page bug in the ORIGINAL engineered-wood
   (09ce9c1). **A/B TEST IN FLIGHT (2026-07-18):** submodule switched BACK to the cmettler fork @ 13fead6
   to see if its writer fixes clear it — dbsp-net main `b99d66a` (".gitmodules→cmettler + pin 13fead6";
   builds + 4 connector round-trip tests green against the fork; path_in_schema override unaffected since
   it's DbspNet-side), ivm-bench `dbspnet-engine` `3ba6863` bumps DBSPNET_COMMIT→b99d66a. Awaiting Curt's
   re-run: if the fork reads dim_customer, cherry-pick the specific fix into CurtHagenlocher/engineered-wood
   (don't keep the whole fork) then revert the submodule; if not, revert to 09ce9c1. Bundle with the
   deferred "flip OmitPathInSchema default false upstream." NOTE possible link to #2 (same parquet stack).
   **A/B RESULT (2026-07-18 re-run @ b99d66a): the FORK FIXES it — dim_customer now READS (6442 rows, no
   more snappy error).** Fix is real, in the fork's writer changes. TODO: bisect the fork to the specific
   commit/hunk, cherry-pick into CurtHagenlocher/engineered-wood, revert the submodule off the whole fork.
   (#2 market_volatility did NOT improve — 1796 vs 5550 unchanged — so #2 is INDEPENDENT of #5, not the same
   root cause.) **NEW gap now that dim_customer reads: data_diff=11866/6442 — a near-total dim_customer
   correctness diff, previously hidden; run diff_columns to characterize (likely SCD-2 or a systematic col).**

**RE-RUN @ b99d66a SCORECARD (2026-07-18):** #1 DECIMAL cast LARGELY cleared trade_volume_stats (354→56
residual) + broker_performance avg part; #4 NULL collation cleared broker_performance ranks AND
customer_concentration (1126→0). CLEAN (data_diff=0, 9 views): dim_broker, dim_company, dim_date,
dim_security, fact_cash_balances, fact_cash_transactions, fact_holdings, fact_trade, customer_concentration.
RESIDUAL to chase: dim_customer 11866 (new/now-visible), trade_volume_stats 56, broker_performance 2.
Unchanged: market_volatility 3778 (#2), dim_account 4 / dim_trade 12 / fact_watches 12 (#3 caveat).

**RESIDUALS CHARACTERIZED via diff_columns (2026-07-18):**
- **dim_customer 11866 = 100% PHONE formatting** (phone1/2/3 diffs 9014/9660/11076) + 4 effective_timestamp
  (#3 SCD-tie family). Every other column (name/addr/email/tax/dob/net_worth/…) matches EXACTLY. Phone is
  built `CONCAT_WS('-', CAST(C_PHONE.C_CTRY_CODE AS VARCHAR), CAST(C_AREA_CODE AS VARCHAR), C_LOCAL, ...)`
  in crm_customer_mgmt.sql (IDENTICAL dbspnet vs feldera). Feldera phones have a LEADING DASH (`-003-6990`),
  dbspnet don't (`003-...`). CONCAT_WS skips NULL but keeps '' — VERIFIED DbspNet's CONCAT_WS is CORRECT
  (BuiltinScalarFunctions.cs:1063 keeps empty Utf8String). So root cause is UPSTREAM: `CAST(nested
  C_CTRY_CODE AS VARCHAR)` = NULL in DbspNet vs '' in Feldera → a NESTED-FIELD null-vs-empty-string
  difference on missing country/area codes (connector/nested-read layer, same family as the nested-struct
  flattening). COSMETIC (phone formatting only); which engine is "right" needs the raw nested value (mount/raw
  wiped). NOT a computation bug.
- **trade_volume_stats 56 + broker_performance 2 = 4th-decimal ROUND residual → FIXED (dbsp-net fe45c05,
  pushed 2026-07-18).** #1 cleared the big errors; remainder was round-half tie-breaking. ROOT CAUSE: DbspNet's
  ROUND inherited .NET Math.Round's DEFAULT = banker's (MidpointRounding.ToEven); SQL ROUND convention
  (Calcite/Postgres-numeric/SQL Server/Oracle/DuckDB/Spark) is HALF-AWAY-FROM-ZERO. Fixed: AwayFromZero on
  SqlBuiltinRuntime.Round (decimal/double/float) + custom Int128-mantissa half-away in DecimalRuntime.Round
  (Clast.DatabaseDecimal Rescale128 is half-even, package can't be changed); both compile paths route through
  these. CAST-to-decimal rounding left as banker's (out of scope). Tests flipped (Round_HalfAwayFromZero +
  _Negative), full suite 2014 green.
  **CORRECTION (2026-07-20, confirmed on a genuine 355ef65/post-fe45c05 Docker run):** fe45c05 did NOT
  clear trade_volume_stats (still 56) or broker_performance (still 2). The residual is NOT a ROUND-MODE
  bug — it's a **4th-decimal FLOAT-BOUNDARY** difference. diff_columns shows every divergent avg_* value
  off by exactly ±0.0001 (72.7638 vs 72.7637, 9.62 vs 9.6199, 5.4581 vs 5.4580). Both engines now round
  half-away; the PRE-round double `CAST(SUM(DECIMAL) AS DOUBLE)/COUNT` lands a sub-ULP apart and straddles
  the .00005 boundary. DbspNet's CAST = `(exact mantissa)/10000.0` (single correctly-rounded div, can't be
  more accurate) → DbspNet is ≥ as correct as Feldera; these are Feldera-side boundary artifacts, same
  "DbspNet more-correct" class as #3. NOT cheaply fixable (would need to replicate Calcite's exact float
  eval). fe45c05 still correct+kept (fixed the clean half-even cases); just doesn't touch the boundary
  residue. Treat as a documented float caveat, do NOT chase bit-identical rounding.

**NET after this run: the only SUBSTANTIVE remaining gap is #2 (market_volatility input row loss).** Everything
else is cosmetic (dim_customer phone null/empty), minor (ROUND half-even), or an accepted caveat (#3).

**PACKAGING SWAP + PIN BUMP (2026-07-20): engineered-wood is now a nuget.org dependency, submodule GONE.**
Curt published engineered-wood to nuget.org @ 0.1.0 (packages share the project names) WITH all the fixes
(the fork's snappy/empty-page writer fix `792c3cf`, path_in_schema, etc.). dbsp-net `355ef65` (pushed to
main): removed the `external/engineered-wood` git submodule + `.gitmodules` + the `external/` CPM/analyzer
isolation props; `DbspNet.Connectors.EngineeredWood` now `PackageReference`s `EngineeredWood.DeltaLake.Table`
+ `EngineeredWood.Parquet` @ 0.1.0 (transitively pull DeltaLake/Expressions(.Arrow)/Core; verified on
nuget.org with correct dep metadata); scoped `#pragma warning disable EWPARQUET0002` on the OmitPathInSchema
override (released package marks it `[Experimental]`); Server Dockerfile + design-connectors.md de-submoduled.
Suite 2014 green (incl. 33 connector round-trip tests) against the packages. **355ef65 sits ON TOP of fe45c05
(the ROUND half-away-from-zero fix), so this build also carries that** → the re-run should clear the
trade_volume_stats 56 / broker_performance 2 ROUND residual too. ivm-bench `dbspnet-engine`: DBSPNET_COMMIT
bumped b99d66a→355ef65 in BOTH `docker/docker-compose.benchmark.dbspnet.yml` AND
`src/containers/dbspnet/Dockerfile` (compose default shadows the Dockerfile ARG — must update both);
submodule-init step dropped (packages restore during `dotnet publish`). ivm-bench commits LOCAL on
`dbspnet-engine`, not pushed. **RE-RUN DONE + VALIDATED (2026-07-20, genuine 355ef65 build — first
attempt accidentally re-ran b99d66a because WSL compose hadn't picked up the pin bump; the tell was the
`docker compose build` clone step RE-RUNNING at 355ef65 instead of CACHED. Landmine: confirm the WSL
checkout's compose pin + that the dbspnet clone shows the intended commit before trusting a run).**
NO REGRESSIONS: the 9 clean views stay clean; #1 avg_* magnitude errors gone, #4 ranks clean (all rank_*
=0), #5 dim_customer reads (6442). Residuals on the 355ef65 run = trade_volume_stats 56 / broker_performance
2 (FLOAT-boundary, see CORRECTION above — NOT the ROUND fix), dim_customer phone (cosmetic) + 4 eff_ts
(#3), market_volatility #2 (1796 vs 5550, input-layer, unchanged). Packaging swap CONFIRMED good.

**SUGGESTED SESSION SPLIT:** (A) DECIMAL agg #1 [next, highest ROI]; (B) DbspNet correctness:
market_volatility #2 (+ maybe SCD #3 / RANK #4); (C) engineered-wood: snappy #5 + OmitPathInSchema
default. Related: [[ivm-bench-arc]], [[engineered-wood-path-in-schema]], [[docker-runs-in-wsl]]
