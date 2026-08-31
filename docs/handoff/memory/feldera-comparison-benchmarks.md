---
name: feldera-comparison-benchmarks
description: DONE — DbspNet-side Feldera-compatible comparison benchmarks (Nexmark + fraud detection)
metadata: 
  node_type: memory
  type: project
  originSessionId: 9ce32518-7b40-4489-b079-f7b54a95e2cf
---

DONE: implemented the DbspNet side of the Feldera vs DbspNet performance comparison. Plan lives OUTSIDE the repo at `C:\src\GitHub\journal\research\dbsp\performance_test.md`.

Added to `src/DbspNet.Benchmarks` (not a new project):
- `Nexmark/` — `NexmarkGenerator` (faithful 1:3:46 Person/Auction/Bid stream, ids@1000, categories 10–14, recency-biased bid→auction FKs so q4/q9 BETWEEN windows are non-empty), `NexmarkQueries` (DDL + q0–q4, q9), `NexmarkBenchmark` (events/sec throughput).
- `Fraud/FraudBenchmark` — customers⋈transactions with rolling 1d/7d/30d COUNT/SUM windows; measures per-event incremental latency (the headline, ~13–28µs) + throughput.
- `ComparisonBenchmarks.cs` dispatcher; sub-commands `nexmark` / `fraud` / `comparison` in `Program.cs`. Writes `docs/benchmarks-comparison.md`.

Key facts learned:
- Each `PlanToCircuit.Compile` = ONE output; `q.Current` is the LAST step's output **delta**, not the integrated view (so the report's "Last Δ rows" column is a smoke signal, not view size).
- q9 needs `WHERE rn <= 1` (NOT `= 1`) — partitioned ROW_NUMBER TOP-K requires a `<= k`/`< k` filter.
- q5/q7/q8 deferred: need TUMBLE/HOP window table functions DbspNet lacks. See [[window-aggregates]] (OVER frames exist; tumbling/sliding table funcs don't).
- Generator is workload-shape-faithful, NOT byte-exact vs Feldera's RNG — good for throughput, not for diffing output rows.

Runs Feldera-compatible SQL; the Feldera side (Rust-native Nexmark / pipeline-manager) still needs running on a matching host to complete the comparison.

**BENCHMARK HOST (durable workflow fact):** Curt runs BOTH the DbspNet and Feldera comparison benchmarks on a SEPARATE (non-Windows) machine, because **Feldera does not compile cleanly on Windows**. The comparison artifacts `D:\src\dbsp-bench-2.txt` (single-core) and `D:\src\dbsp-bench.txt` (multi-core) come from that machine. **So running DbspNet-side Nexmark on THIS (Windows) box has little comparison value** — it's not apples-to-apples vs Feldera (different host). Do NOT offer to regenerate comparison numbers here; the comparison re-run is something Curt does on the other machine. (Windows-side benchmarks are still useful for same-box A/B gates like `w1profile`/`q4prune`/`reprbench`, just not for the Feldera ratio.)

**Nexmark query coverage expanded q0–q4,q9 → +q17,q18,q19,q20 (2026-06-09; commit 52d63be). Now 10 queries.** All four needed NO new engine features, only DbspNet-dialect authoring (added to `NexmarkQueries.All`, which BOTH `NexmarkBenchmark` (nexmark/comparison cmd) AND `SpineEvalBenchmark` (spineeval) iterate → auto-wired). q17 = per-auction/day stats: `GROUP BY (auction, CAST(date_time AS DATE))` + `SUM(CASE WHEN … THEN 1 ELSE 0 END)` for Feldera's `COUNT(*) FILTER(WHERE …)` conditional counts + MIN/MAX/AVG/SUM. q18 = dedup `ROW_NUMBER() OVER (PARTITION BY bidder,auction ORDER BY date_time DESC) rn<=1` (multi-col PARTITION BY — new vs q9's single-col). q19 = auction TOP-10 `ROW_NUMBER … PARTITION BY auction ORDER BY price DESC rn<=10`. q20 = filtered `bid⋈auction WHERE category=10`. Validated: `tests/DbspNet.Tests/Sql/NexmarkNewQueriesTests.cs` compiles+asserts output on hand-built data (suite 1684); `nexmark` W=8 smoke → q18/q19/q20 parallelize (W1≡W8 "ok"), **q17 is single-only** (no parallel form — computed group key CAST(…) + CASE aggregation aren't handled by the typed PARALLEL exchange path; correct single-threaded). Timestamp/Date inserts in tests: `new Timestamp(micros)` / `new Date32(days)` (DbspNet.Sql.TypeSystem).

**NEXMARK GAP ROADMAP (from §… gap analysis, verified vs codebase + Feldera's actual SQL):**
- **Small features (next, each bounded):** (1) ~~exact `COUNT(DISTINCT x)`~~ **DONE** (see [[count-distinct-exact]]) → q15,q16. (2) ~~`SPLIT_INDEX`/`SPLIT_PART`~~ **DONE** (registry entries in ScalarFunctionLibrary; see [[scalar-function-registry-temporal]]) → q22 enabled. Nexmark coverage now **13** (q0–q4,q9,q15–q20,q22). (3) ~~`FILTER (WHERE …)` agg clause~~ **DONE** (commit 567c2ed) — parse-time sugar in Parser.cs: `agg(x) FILTER(WHERE p)`→`agg(CASE WHEN p THEN x END)`, `COUNT(*) FILTER`→`COUNT(CASE WHEN p THEN 1 END)`, preserves DISTINCT; flows through CASE machinery + parallelizes; `filter` contextual keyword; window form rejected. Tests: FilterClauseTests. **q15/q16/q17 benchmark SQL still uses the hand-rewritten SUM(CASE…) form — can be switched to verbatim Feldera FILTER SQL now (identical results; COUNT(*) FILTER→COUNT(CASE) is a slightly different plan shape than the current SUM(CASE 1/0), so do it between comparison runs, not mid-run).** **NEXT = windowing TVFs (big gap below).**
- **Big gap = windowing TVFs TUMBLE/HOP/SESSION** → q5,q7,q8,q11,q12. Biggest item, design-doc-worthy (window-assignment operator fanning rows into window keys + watermark GC). TUMBLE partly emulable via `GROUP BY floor(epoch/W)*W` but join-to-window (q7) / proctime (q12) make faithful rewrites non-trivial; HOP (overlapping) & SESSION NOT reducible to GROUP BY. Feldera also omits q11(session).
- **Out of scope:** q6 (Feldera omits), q10 (sink), q13 (side input), q14 (verify UDF count_char), q21 (UDF — Feldera omits). Feldera itself omits q6,q10,q11,q21.
- Reachable totals: 13 now (COUNT(DISTINCT)→q15/q16, SPLIT→q22) → ~17 once windowing lands.
