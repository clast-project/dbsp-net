---
name: approx-quantiles
description: "DONE: approximate quantiles (DDSketch) — APPROX_PERCENTILE/MEDIAN/PERCENTILE_CONT/DISC shipped on main; the second sketch aggregate, fully invertible"
metadata: 
  node_type: memory
  type: project
  originSessionId: e30ec68e-f58d-4691-b332-683be530cd98
---

DONE (shipped to main): approximate quantiles — the second approximate/sketch
aggregate after [[approx-count-distinct]], the remaining open item under #2 of
[[roadmap-candidates]].

**SQL surface (user chose "both"):** `APPROX_PERCENTILE(x, f)` + `MEDIAN(x)`
(function-call form) AND the ANSI ordered-set spellings `PERCENTILE_CONT(f)
WITHIN GROUP (ORDER BY x)` / `PERCENTILE_DISC` — all four lower to one
`AggregateKind.ApproxPercentile`. PERCENTILE_DISC is treated identically to
_CONT (both approximate via the sketch). Numeric args only; result is nullable
DOUBLE (NULL for empty/all-NULL group, like MIN/MAX).

**Sketch — DDSketch** (`DbspNet.Core/.../Aggregators/DdSketch.cs`, mirrors
HyperLogLog.cs): relative-error bucketed histogram, bucket key =
ceil(log_γ|x|), γ=(1+α)/(1−α), α=1% default; representative `2·γ^i/(γ+1)`
(GOTCHA: factor is `2/(γ+1)`, NOT `2γ/(γ+1)` — the extra γ inflates every
estimate ~2% and silently violates the α bound). Separate negative store + zero
count. **Fully invertible**: signed per-bucket counts, so `Add(v, weight)` with
negative weight retracts (bucket dropped at 0) — better incrementality than HLL.
Every tick folds the signed delta into the running sketch (the SUM/COUNT
pattern, NOT HLL's rebuild-on-retraction), and because the bucket map is a
deterministic function of the present multiset, **incremental ≡ batch exactly**
(PBT asserts exact double equality). State bounded by dynamic range
(log_γ(max/min)), not cardinality; no bucket collapse.

**The new infrastructure vs APPROX_COUNT_DISTINCT — the fraction (2nd arg):**
aggregates previously hard-threw on anything but 1 arg. Added `double? Fraction`
to `AggregateCall` and `AggregateKey` (so two percentiles of the same expr get
distinct slots). Resolver: new `ResolveAggregateArgs` / `ResolvePercentileArgs`
/ `ReadFractionLiteral` helpers centralize arg+fraction extraction at the two
GROUP BY binding sites; MEDIAN→0.5, others take a literal fraction in [0,1]
(Integer/Float via Convert.ToDouble w/ InvariantCulture, Decimal via
Mantissa/10^scale). Value→double in `DdSketchSupport.ToDouble(value, decScale)`
(decScale captured from the arg's SqlDecimalType at compile time, 0 otherwise).

**Wiring checklist (same switch sites as HLL, see [[approx-count-distinct]]):**
enum in LogicalPlan.cs; IsAggregateName/ToAggregateKind/ComputeAggregateResultType
(4 names → ApproxPercentile, DOUBLE nullable); PlanToCircuit.BuildSqlAggregator;
BatchPlanEvaluator mirror switch; TypedPlanCompiler (TypedAggregateResultType +
BuildApproxPercentileAggregator boxing extractor + fraction/scale). **Difference
from HLL:** percentile uses signed weight-SUM semantics → it is *linear* like
SUM/COUNT, so it must NOT bail in PlanOptimizer.NarrowAggregateInput (HLL/MIN/MAX
DO bail; narrowing is sound for percentile). Window-function form rejected in the
resolver (OVER path). Parser: new `ParseWithinGroup` (contextual `within`
keyword, percentile_cont/disc only) desugars to `(value, fraction)` call shape.
`ORDER BY … DESC` in WITHIN GROUP IS supported: parser lowers it to `1 − f`
(BinaryExpression Subtract) and ReadFractionLiteral/ReadNumericLiteral
constant-folds `literal − literal`, so resolver+sketch stay ordering-agnostic.

Tests: `tests/.../Operators/Stateful/DdSketchTests.cs` (Core: ramp/negatives/
multiplicity/determinism/invertibility/merge within α bound) +
`tests/.../Sql/ApproxPercentileTests.cs` (typed/structural/spine, median,
within-group, group by, nulls, NULL-result, deletes, decimal, large-N,
incremental≡batch exact PBT, DESC within-group inverts fraction, resolver/parser
error cases). Deferred: APPROX_QUANTILES(x,n) array form; heavy-hitters
(Count-Min) still P2. Temporal-typed quantiles now DONE — see
[[temporal-quantiles]].
