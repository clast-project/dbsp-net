---
name: interval-datetime-arithmetic
description: "DONE — INTERVAL type + date/time arithmetic core, shipped to main"
metadata: 
  node_type: memory
  type: project
  originSessionId: 24889398-8fbe-40c3-887a-de443a383fa4
---

DONE (shipped to main, commit 15125b6): INTERVAL type + date/time arithmetic core.

- `Interval(int Months, long Micros)` value struct in `TemporalValues.cs` — carries
  SQL's two interval classes (year-month / day-time) side by side; class travels on
  the type via `IntervalQualifier` + `SqlIntervalType`.
- `INTERVAL '..' <unit>` literals **desugar at parse time to `CAST('..' AS INTERVAL <unit>)`**
  (no new AST node — every walker is transparent); resolver constant-folds the cast to a
  typed `ResolvedLiteral(Interval)`. Only `INTERVAL` is a new reserved keyword; field words
  (day/month/…) stay non-reserved identifiers.
- Arithmetic in `Resolver.ResolveTemporalArithmetic` + structural `ExpressionCompiler`
  (helpers in new `TemporalArithmetic.cs`): date/time/ts ± interval (calendar-aware month
  add; **DATE arithmetic is day-granular**), interval ± interval (same class), interval ×/÷
  numeric, date−date / ts−ts / time−time → interval. string↔interval CAST.
- **Typed fast path falls back to structural** for temporal/interval ops automatically
  (`BuildNumericArith` throws Unsupported when result CLR type isn't numeric) — same as
  temporal comparisons already do. `BatchPlanEvaluator` reuses structural, so batch≡incremental.

23 tests in `IntervalTests.cs`; full suite 1189 pass / 1 skip.

Deferred follow-ons: INTERVAL **stored columns** through the Arrow codec (intervals are
intermediate-only today — a persisted/snapshotted interval output column needs an Arrow
`MonthDayNano` mapping); `interval × decimal`; typed-fast-path temporal arithmetic.

Relates to [[dbspnet-overview]]; the deferred scalar-function registry (`docs/scalar-function-registry.md`)
is the right home if/when temporal *functions* (EXTRACT, DATE_TRUNC, DATEADD) get added next.
