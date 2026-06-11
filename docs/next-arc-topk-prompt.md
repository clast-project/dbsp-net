# Starting prompt — next arc: q18/q19 partitioned TOP-K, the last competitive gaps

> Ready-to-paste kickoff for the arc the §21 + typed-ingest-scoping session pointed at.
> q4 is fixed (§21 join column pruning, default-on); typed ingest is retired by
> measurement. After Run D (2026-06-11) the only clear competitive losses left are
> **q18 (0.46× @10c) and q19 (0.52× @10c)** — and at single-core they are the *worst*
> queries (q19 0.32×, q18 0.34×). This prompt seeds a fresh session at the real
> decision — *is any part of the q18/q19 gap addressable, or is it the fundamental
> coordination/per-row floor?* — instead of re-deriving the arc. Paste the block below
> into a new session. Use Opus.

---

**Partitioned TOP-K (q18/q19) — the last competitive gaps: measure-first, and be willing to retire it as the coordination/per-row floor**

The per-row / representation arc (`docs/design-row-representation.md` §16–§21; memories
`[[per-row-execution-efficiency]]`, `[[nexmark-feldera-w14-snapshot]]`,
`[[join-column-pruning]]`) closed q4 — the worst single-core gap — with §21 projection
pushdown through joins (now default-on, q4 0.21→0.42× single-core, **1.05× @14c, ahead
of Feldera**). The latest comparison (Run D, `[[nexmark-feldera-w14-snapshot]]`) shows
DbspNet now wins or ties most of Nexmark. **The only clear remaining losses are the two
partitioned-TOP-K queries:**

| | 1c | 10c | 14c | shape |
|--|--|--|--|--|
| q18 | 0.34× | **0.46×** | 0.47× | dedup latest bid, `ROW_NUMBER() PARTITION BY bidder,auction ORDER BY date_time DESC`, rn≤1 |
| q19 | 0.32× | **0.52×** | 0.62× | top-10 bids/auction, `PARTITION BY auction ORDER BY price DESC`, rn≤10 |

**Read before proposing anything — the honest opening (do NOT skip):**

1. **This arc may retire. Two prior results bound it hard.** (a) §19 already tried the
   obvious in-`Step` lever — narrowing the `PartitionedTopKOp` window container
   (dict→array) — and **reverted it**: mixed wins, and it *regressed q9 +14% alloc* for
   reasons that were never explained. Do **not** re-try a TOP-K *container* change. (b)
   The q18 profile (`q18profile`, `docs/q18-profile.md`, recorded in
   `[[nexmark-feldera-w14-snapshot]]`) found q18 is **STEP-bound, NOT gather-bound**:
   gather/output materialization is ~5ms (negligible — the parallel path decodes output
   lazily off-`Step`, §16.10), and the cost is *inside* `Step` — the wide-row
   inter-worker **exchange** (shuffling whole 7-column bid rows to partition) +
   coordination, which §15 argued is **substantially fundamental** and §16.11 found is
   our **strength, not a leak** (we out-scale Feldera; coordination is not a target). So
   a real chance this arc concludes "the residual is the BSP coordination + per-row
   floor; narrow it modestly or accept it." Say so honestly if the measurement says so.

2. **q18/q19 are NOT column-prunable the way q4 was.** Both `SELECT auction, bidder,
   price, channel, url, date_time, extra` = *all 7 bid columns* (effectively `SELECT *`),
   so §21's projection-pushdown lever does not transfer — the output genuinely needs
   every column. This is the **genuinely-wide residual** §21.6 scoped columnar to.

3. **Typed ingest is dead** (retired this session, `ingestpath` / `docs/ingest-path-bench.md`):
   the parallel path already encodes `object?[]`→`ZSet<TRow>` directly, and the
   single-circuit typed-vs-structural A/B was ~parity (0.85–1.16×). Do not re-propose it.

**The decomposition the measure-first step must produce (before any lever):** split the
q18/q19 gap, at **single-core AND at W=10/14 separately**, into (a) in-`Step` per-row
TOP-K *state* cost (the `SortedDictionary`/window over wide retained rows), (b) in-`Step`
wide-row **exchange** (shuffle by partition key — q18profile's suspected multi-core
culprit), (c) **coordination/barrier** wait (the §15 BSP ceiling — *not a target*), (d)
out-of-`Step` output materialization (q18profile says ~0 — confirm it's still ~0 post-§21).
Extend `q18profile`/`StepProfiler` to attribute these; do the same for q19. **Pick the
lever — or retire — by this evidence, not assumption.**

**The leading candidate lever, IF (a)+(b) are large and (c) is not the whole story
(name it, but measure first):** *narrow what TOP-K moves and retains, recover the wide
output for survivors only.* The ranking decision needs only `{partition keys, order
keys}`; the other ~4 columns (price/channel/url/extra) are dead weight in the exchange
shuffle and the retained window — needed *only* for the handful of survivors (q18: 1 per
(bidder,auction); q19: 10 per auction — a tiny fraction of input bids). So: exchange +
rank on a narrow `{partition, order, row-ref}` projection, then materialize the wide
output rows only for the survivors. This attacks (a) the retained-state width *and* (b)
the exchanged-bytes width at once — single-core *and* multi-core — and is **different
from §19's reverted container change** (it's a row-width / fetch-back architecture, not a
dict→array swap). The hard part is the survivor→wide-row recovery (a join-back / row
identity), and whether it pays once the partition is already co-located on a worker.
Be honest about whether it beats just storing the wide row, and gate it.

**Deliverable:** a design note (`docs/design-row-representation.md` §22) + the
**decomposition measurement** (the §1 deliverable, durable) + the **smallest
benchmark-gated first increment** only if the evidence justifies one — convert q18 (the
simpler, TOP-1 case) behind a seam (mirroring `JoinColumnPruningMode` /
`NonLinearNarrowingMode`), gated on q18 single-core `w1profile` + W=8 step
(`q18`-analogue of `q4prune`/`SpineParallelHarness`) with the per-tick output
cross-check, retiring it if it loses (as §19's container change did). **No broad change
before the gate.**

**Respect / landmines.**
- **Preserve q3 (3.19× single-core) and the W>1 wins** — any TOP-K change must be
  seam-gated to the partitioned-TOP-K path, never a universal tax.
- **Coordination is NOT a target** (§16.11 — it's our strength; q18 is step/exchange/
  coordination-bound per q18profile). If the decomposition says (c) dominates, the
  honest outcome is "narrow (a)/(b) modestly, accept the BSP floor," not a coordination
  rewrite.
- **Honor the typed-compiler reflection gotcha** (`[[typed-compiler-reflection-gotcha]]`):
  q18/q19 run the typed parallel path; reach any new representation via an ambient
  `[ThreadStatic]` seam at Optimize/construction time, **not** a builder-signature change.
- **Retired by measurement — do not revive:** typed ingest (this session), surrogate
  keys (§14.9), whole-query codegen (§17.2), sorted-merge/spine storage on fine ticks
  (§8.3), and §19's TOP-K *container* change. "Columnar" here = narrower moved/retained
  rows + survivor fetch-back, NOT a new sorted store.
- **Honest ceiling (§17.5):** a managed engine narrows the 2–5× single-core laggards
  toward ~1.3–2×, not parity. q18/q19 at 0.32–0.34× single-core have headroom, but
  parity with monomorphised Rust over a `SortedDictionary` of wide rows is not the bar.

**Read first:** `docs/design-row-representation.md` — **§15** (the exchange/coordination
ceiling + StepProfiler), **§16.10/§16.11** (out-of-`Step` output is W>1-only; coordination
is a strength), **§19** (the TOP-K window-rep dead-end — *why not there*), **§21** (the
join-pruning win + the genuinely-wide-residual framing). Memories
`[[nexmark-feldera-w14-snapshot]]` (Run D + the q18 profile finding),
`[[per-row-execution-efficiency]]` (the typed-ingest retirement + the Layer-A/B split),
`[[join-column-pruning]]`, `[[exchange-scaling-decomposition]]`,
`[[parallel-pipeline-perf]]`. Code: `Operators/Stateful/PartitionedTopKOp.cs` (the window
+ `ComputeWindow`/`EmitDiff`), `ExchangeIndexOp`/`ExchangeOp` (the wide-row shuffle),
`Circuit/ParallelCircuit.cs`/`ExchangeCoordinator.cs`/`StepProfiler.cs`,
`TypedPlanCompiler` (the parallel typed path), `Sql/Optimizer/JoinColumnPruningMode.cs` +
`PruneJoinInputs` (the §21 seam pattern to mirror). Tooling already built & reusable:
`q18profile` (`Q18ProfileBenchmark`, split/step/gather W-sweep — **extend it to the
4-way decomposition above, and add q19**), `stepprofile`/`StepProfiler`, `w1profile`,
`reprbench`, `SpineParallelHarness` (W=8 gates + output cross-check). Comparison data is
external (Feldera won't build on Windows — see `[[feldera-comparison-benchmarks]]`); the
latest is Run D in `[[nexmark-feldera-w14-snapshot]]`. Run same-box A/B gates
(`w1profile`/`q18`-style) on Windows; the Feldera ratio re-run happens on the other box.
