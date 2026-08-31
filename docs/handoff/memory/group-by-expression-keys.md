---
name: group-by-expression-keys
description: "DONE — GROUP BY accepts arbitrary scalar expression keys (a+b, LENGTH(x), CAST(ts AS DATE)); lifted the v1 bare-column-only restriction"
metadata: 
  node_type: memory
  type: project
  originSessionId: 34408679-4adb-4be5-aa57-d35e1938a715
---

DONE (2026-06): lifted the "GROUP BY supports only bare column references in v1"
restriction in `Resolver.cs`. GROUP BY now accepts any scalar expression key
(`a + b`, `LENGTH(name)`, `CAST(ts AS DATE)`); aggregates in a key are rejected
(`HasAggregate` guard). This was the [P1] in `docs/skipped.md` (now flipped to
Done).

How it works:
- **Resolver** (`ResolveSelect` GROUP BY block): a bare `ColumnReference` keeps
  its name/qualifier (back-compat); any other expr is `ResolveScalarExpression`'d
  and gets a synthetic `$gk{i}` output column name. `groupKeyAstItems` still holds
  `(Ast, OutputIndex)` (no Type added — see below).
- **Post-aggregate matching**: `ResolvePostAggregateExpression` matches a
  SELECT/HAVING sub-tree against a group key via `AstEqual`, then returns
  `ResolvedColumn(OutputIndex, ResolveScalarExpression(expr, preSchema).Type)` —
  re-resolving the matched sub-tree against the pre-aggregate schema to get its
  type (avoids threading the type through the 5 `Build*Post` helpers).
- **Compile**: both `PlanToCircuit.CompileAggregate` AND
  `BatchPlanEvaluator` (the oracle — must stay in lockstep!) now rekey by running
  `ExpressionCompiler.CompileScalar` delegates per group key and `BuildRow` on a
  synthetic key schema, instead of pattern-matching `ResolvedColumn`/column
  indices. Same model as JOIN/IN composite equi-keys.

**Gotcha (the bug that bit):** `AstEqual` was `a.Equals(b)` (record auto-equality).
Nodes with collection members (`FunctionCallExpression.Arguments`,
`InListExpression.Values`, `CaseExpression.Whens`) compare those lists **by
reference**, so two separately-parsed `LENGTH(name)` were unequal → SELECT key
didn't match GROUP BY key → "column must appear in GROUP BY". Fix: `AstEqual` is
now an explicit recursive structural walk comparing list members element-wise.
Arithmetic (`a+b`) worked before the fix because `BinaryExpression` of columns
recurses through value-equal records; only list-bearing nodes broke.

**Closes the loop with [[temporal-filters-now]]:** `GROUP BY CAST(ts AS DATE)`
now works directly (no derived-table workaround). The `MonotonicityAnalyzer`
AggregatePlan arm already ran `FromExpr(GroupKeys[g])`, so a monotone CAST key
automatically picks up the temporal filter's day-space GC frontier — bounded
state for free. Verified by PBT shapes (direct + derived-table GROUP BY over a
CAST(ts AS DATE) filter) that would fail on unsound GC via incremental≠batch.

**Deferred:** group keys containing subqueries/window functions (fall through to
record identity in AstEqual). Relates to the general scalar-expression machinery.

**UPDATE (June 2026, commit 6c5b43c): the TYPED compile path now also supports
expression group keys** (was structural-only — typed `CompileAggregate` used to
bail on any non-`ResolvedColumn` key). `BuildExprKeyExtractorDelegate` lowers each
key expr into the key row; the parallel exchange shards on the computed key →
`GROUP BY CAST(ts AS DATE)` etc. now parallelize (unblocked Nexmark q15/q16/q17,
see [[parallel-pipeline-perf]]). **Sibling of the AstEqual gotcha, one layer down:**
`AggregateKey` deduped its argument by RECORD equality, which reference-compares
`ResolvedCaseWhen.Whens` / `ResolvedFunctionCall.Arguments` / `ResolvedInList.Values`
— so `SUM(CASE…)`/`COUNT(DISTINCT CASE…)` re-resolved at the 2nd collection site
collected the aggregate TWICE. Fixed with a structural `ResolvedExprEqual`
(resolved-layer twin of AstEqual) + custom AggregateKey Equals/GetHashCode.
