# Design note: `NOW()` / `CURRENT_TIMESTAMP` and temporal filters

**Status:** **implemented (option B — advancing temporal filters).** Shipped
2026-05/06 in five phases on `main`; `CURRENT_DATE` (day-truncated clock) shipped
2026-06 and `CURRENT_TIME` is rejected as cyclic (see
"`CURRENT_DATE` (shipped) / `CURRENT_TIME` (rejected)"). This note keeps the original design
rationale (why `NOW()` is not a registry entry, the correctness-oracle problem,
the three features the keyword could mean) below, and records the as-built
shape in **"Implementation status (shipped)"** immediately following. Captured
after the scalar-function registry (`scalar-function-registry.md`) and the
temporal function / LATENESS-GC work landed.

## Implementation status (shipped)

Option **B** is implemented: `NOW()` / `CURRENT_TIMESTAMP` is an advancing,
host-driven logical clock, legal only inside sanctioned temporal-filter
predicates, compiled to a time-driven operator that emits retractions as the
clock advances. Option A (per-tick stamping) was deliberately *not* shipped;
option C (frozen constant) was subsumed (a frozen value is just a clock the host
never advances).

- **Clock.** `RootCircuit.LogicalTime` (microseconds since epoch, the `TIMESTAMP`
  unit), advanced by the host via `RootCircuit.AdvanceTime` /
  `CompiledQuery.AdvanceClock` before each `Step`; monotone non-decreasing
  (backward move throws); `long.MinValue` is "unset" (logical −∞). Never reads
  the wall clock. Persisted in the snapshot manifest (`logical_time`, schema
  v3) and restored *before* operators load. Exposed as an `IFrontier`
  (`RootCircuit.Clock`) so it unifies with the LATENESS frontier machinery.
- **Grammar.** A dedicated `NowExpression` AST node (never a
  `FunctionCallExpression` — it bypasses the purity-contracted registry).
  `NOW()` needs its empty arg list (a column named `now` still resolves);
  `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` are parenless. Recognised
  only in WHERE conjuncts of the form `key {<|<=|>|>=} clock [± constant day-time
  INTERVAL]` (both operand orders; `BETWEEN` folds into one two-bound window).
  The clock anywhere else — SELECT, projection, HAVING, disjunctions, a key whose
  type doesn't match the clock, a month/year offset, both sides, `=` — is a
  `ResolveException`. Only day-time intervals are valid offsets (they are a
  constant number of microseconds; month/year are not). `CURRENT_DATE` takes a
  `DATE` key with whole-day offsets (see below); `CURRENT_TIME` is rejected
  outright as cyclic.
- **Plan / operator.** The resolver folds qualifying conjuncts into a
  `TemporalFilterPlan` (per-row validity window affine in the time-key);
  `PlanToCircuit` compiles it to `TemporalFilterOp<TRow>`, which integrates the
  input and each tick recomputes the valid set against the clock and emits the
  delta against the previously-emitted set — so rows age in/out (including on
  input-free ticks where only the clock moved). `ISnapshotable`. The typed fast
  path falls back to structural, as for LATENESS.
- **Correctness oracle.** A temporal filter at logical time `t` is exactly the
  relational filter with `NOW = t`. `BatchPlanEvaluator` gained a `Now` and a
  `TemporalFilterPlan` arm; the random-query oracle drives the circuit advancing
  the clock per tick, accumulates the output, and checks equality with the batch
  evaluated at the run's *final* clock (the emitted deltas telescope to
  `validAt(finalClock)`). See `TemporalFilterPbtTests`.
- **State GC (clock-as-watermark).** The operator self-GCs rows once the clock
  passes their upper bound. A disappear-bounded filter on a bare time-key column
  also advertises a clock-driven frontier (`clock − offset`) on that column,
  reusing the LATENESS source/frontier plumbing, so a downstream `GROUP BY` /
  join / `DISTINCT` on the time-key GCs the same way.

- **`CURRENT_DATE` (shipped).** A `DATE`-keyed temporal filter; the µs clock is
  truncated to its day-number before the comparison, so everything (clock, key,
  offsets) lives in day units. See **"`CURRENT_DATE` (shipped)"** below.
- **`CURRENT_TIME` (rejected).** Cyclic, not monotone, no sound advancing
  semantics — rejected with a clear error. See below.

**Deferred follow-ons:** a `CURRENT_TIME` frozen-constant feature (a separate
design, see below); a spine sibling operator; a per-row transition-time index
(the operator's per-tick recompute is O(integral size)); monotone-*expression*
time-keys (e.g. `date_trunc(ts)`) and filters not directly over a scan, for the
downstream-GC frontier; the typed fast path; and WAL (approach A) per-tick clock
recording for pure-log replay (the snapshot path is correct today).

### `CURRENT_DATE` (shipped) / `CURRENT_TIME` (rejected)

The advancing-clock model is built on a single monotone `Int64` clock in
`TIMESTAMP` units (microseconds since epoch) and a comparison *linear in `now`*
(`now {<|<=|>|>=} timeKey + offset`). The two other niladics each break one of
those assumptions, so each was a decision, not a spelling:

- **`CURRENT_TIMESTAMP` is the clean case.** Clock and key are both
  µs-since-epoch; the comparison is linear in `now`. Nothing below applies.

- **`CURRENT_DATE` is monotone but *truncated* — shipped via a day-space clock.**
  A `DATE` key is days; the clock is µs. `d <= CURRENT_DATE` means
  `d <= floor(now / µs_per_day)`, which is linear in `truncate_to_day(now)`, not
  in `now`. The implementation runs the **whole filter in day-number space**
  (the design's "option 1", chosen because `MonotoneKey.Extract` already reads a
  `DATE` column as its day-number, so the downstream-GC frontier must be in
  day-numbers too):
  - A `TemporalClock` discriminator (`Timestamp` | `Date`) on `TemporalFilterPlan`
    fixes the unit of the clock, the extracted time-key, and the offsets. It is
    set by the resolver from the niladic spelling, which must match the key type
    (`CURRENT_DATE` ⟺ `DATE` key; `NOW()`/`CURRENT_TIMESTAMP` ⟺ `TIMESTAMP` key).
  - The compiler wraps the µs `LogicalClock` in a `TransformedFrontier` with
    `Date32.DayNumberFloor` (`now ⇒ floor(now / µs_per_day)`) and extracts the
    key as `Date32.Days`; offsets are whole days (`CURRENT_DATE - INTERVAL '1'
    DAY` ⇒ offset `1`). **`TemporalFilterOp` itself is unchanged** — it only ever
    compares `long`s in whatever shared unit it's handed, so the day case is just
    a different clock frontier + key carrier.
  - The downstream-GC frontier is `floor(clock / µs_per_day) − offsetDays`
    (`ClockOffsetTransform(TemporalClock.Date, …)`), matching the day-number a
    `DATE` column extracts — so a `GROUP BY`/join/`DISTINCT` on the date key GCs
    by whole days.
  - The batch oracle (`BatchPlanEvaluator.TemporalValid`) applies the same
    day-floor to `now` and reads the key as a day-number, keeping
    incremental-equals-batch (a dedicated `CURRENT_DATE` PBT drives hour-scale
    clock jumps so day boundaries are crossed both within and across ticks).
  - Sub-day `INTERVAL` offsets are rejected: a sub-day shift moves the
    day-truncated clock off a day boundary, which the day-space comparison can't
    represent. Only whole-day (`INTERVAL '<n>' DAY`) offsets are allowed.

  Option 2 ("scale the key to µs, keep the clock raw") was *not* taken: a shifted
  bound (`d > CURRENT_DATE - INTERVAL '1' DAY`) still depends on
  `floor(now/µs_per_day)`, so the clock must be floored regardless — at which
  point day-space is the simpler representation.

- **`CURRENT_TIME` is *cyclic* — rejected.** `TIME` is µs-since-midnight, i.e.
  `now mod µs_per_day`, which wraps every midnight and is therefore **not
  monotone**. The whole feature — forward-only retractions, the GC watermark, the
  telescoping `validAt(finalClock)` oracle — rests on a non-decreasing clock, so
  a `CURRENT_TIME` temporal filter has no sound advancing semantics. It is
  rejected with a clear error everywhere (in a temporal filter and as a general
  scalar): *"CURRENT_TIME is cyclic (it wraps every midnight) and so is not
  monotone; … Use CURRENT_TIMESTAMP …"*. The only meaning it *could* have is a
  *frozen-per-tick constant* (options A/C in the rationale below) — a separate
  feature with its own equivalence caveats, deferred until independently
  justified.

---

## Original design rationale

*(Captured before implementation; retained for the reasoning. Where it says
"proposed", read "shipped as described".)*

Read this before adding `NOW()` / `CURRENT_TIMESTAMP` / `CURRENT_DATE`,
`mz_now()`-style temporal filters, or any other time- or input-independent
source of change to the engine. It is a design-decision note, not an
implementation plan — the central choice (frozen constant vs. advancing
watermark) is a product decision that has to be made first.

## The load-bearing fact

Everything the engine assumes about scalar evaluation rests on one property:

> A scalar function is applied **pointwise** inside a linear `map` / `filter`,
> and the only property required for incremental correctness is **determinism
> (purity)** — the function is a pure function of the row. (See
> `scalar-function-registry.md`, "The load-bearing DBSP fact".)

`NOW()` is **not a pure function of the row.** Its value depends on *when* it is
evaluated, not on the row. That single fact has three consequences, in
increasing order of severity:

1. It cannot be an `IScalarFunction` registry entry — those compile a
   `Func<row, value>` the runtime assumes is pure in the row. `NOW()` has no row
   argument at all.
2. It must be evaluated **once per tick and broadcast**, never re-evaluated
   per row, or two rows in the same logical batch could disagree on "now".
3. It breaks the engine's **correctness oracle** — the "incremental ≡ batch"
   law that the random-query PBT (`RandomQueryPbtTests`, the test of record)
   enforces. This is the deep problem, and it is why `NOW()` is a feature, not a
   function.

## Why the correctness oracle breaks

The PBT asserts: replaying a stream tick-by-tick through the incremental circuit
and accumulating the output deltas equals a single batch evaluation
(`BatchPlanEvaluator`) over the net input. This holds for any pure scalar
function because the batch and the increments compute the *same* function of the
*same* rows.

`NOW()` has no single value a batch can use. Consider `SELECT NOW() AS t, x FROM s`:

- **Incrementally**, row `x1` inserted at tick `N` is stamped with `clock(N)` and
  never revisited (insert-only ⇒ no retraction). Row `x2` inserted at tick `M`
  is stamped with `clock(M)`. The accumulated output carries *different* `t`
  values per row, keyed by arrival tick.
- **In batch**, there is one evaluation and therefore one `NOW()`; every row gets
  the same `t`.

So incremental ≠ batch by construction — not because of a bug, but because the
batch model has no notion of "the tick a row arrived". Supporting `NOW()`
*requires redefining the correctness notion* (e.g. to a bitemporal model where
the oracle knows each row's logical arrival time), or restricting `NOW()` to
positions where the difference is unobservable. This is the crux of the design.

## The design space: three different features wearing one keyword

### C. Frozen constant (evaluate-once, never advances)

`NOW()` is resolved to a single value at query install / first tick and treated
as a **compile-time constant** thereafter. `WHERE created_at > NOW() - INTERVAL
'30' DAY` becomes a fixed cutoff baked in when the view is created.

- **Correctness:** trivially sound — it's a constant, so purity and the batch
  oracle hold (the batch uses the same frozen value). Zero new operators.
- **Semantics:** weak. "Now" is frozen at query start and never moves; a
  long-running view's cutoff drifts stale. Useful for "as-of" / bootstrap
  queries, not for live "last 30 days" windows.
- **Verdict:** a legitimate, low-risk v1 *if* a frozen value is acceptable. Must
  be **persisted** (see below) so a restart reproduces the same constant rather
  than silently re-freezing at a new time.

### A. Per-tick stamping in projections (a trap — do not ship silently)

`NOW()` reads an ambient per-tick clock; a projection `SELECT NOW() AS t, …`
stamps each row with the clock at its arrival tick. Mechanically easy (an
`ApplyOp` transform closing over a mutable clock cell). **But** it breaks the
batch oracle exactly as shown above, and gives users surprising semantics
(output rows carry different `t` with no visible cause). If it is ever offered,
it must be behind an explicit opt-in with the equivalence caveat documented —
not the default meaning of `NOW()`.

### B. Temporal filters / advancing watermark (the real streaming feature)

This is what Materialize (`mz_now()`) and Feldera actually implement, and what
users usually want. `NOW()` is an **advancing logical clock** allowed only in
specific predicate shapes, e.g.

```sql
WHERE event_ts BETWEEN mz_now() - INTERVAL '1' HOUR AND mz_now()
```

compiled to a **time-driven operator** that emits **retractions as the clock
advances, with no new input** — a row that ages out of the window is retracted
on the tick the clock crosses its upper bound. Correctness is defined
*operationally* (the output at logical time `t` is the relational answer with
`NOW = t`), not via the static batch oracle.

- **Power:** this is the feature — live windows, expiry, "recent" views.
- **Cost:** a new operator class (time-driven change), a redefined correctness
  model + new oracle, and direct interaction with the LATENESS / frontier
  machinery (the clock *is* a watermark; an advancing `mz_now()` and an
  advancing lateness frontier are the same kind of object and must cohere).
- **Restriction:** `mz_now()` is **not** a general scalar — it is legal only in
  the sanctioned temporal-filter predicate positions, rejected elsewhere. That
  syntactic restriction is what keeps it sound.

## The clock must be an injected, persisted input — never `DateTime.Now`

Whatever the semantics, the clock cannot be `DateTime.UtcNow` read inside an
operator. Three existing constraints force a driven, deterministic clock:

1. **Snapshot / replay determinism.** `persistence.md` requires restart to
   reproduce identical output. A wall-clock read inside the circuit makes past
   ticks irreproducible. The clock value per tick must be **persisted** (like the
   LATENESS `maxSeen` in `frontier.bin`, or `RootCircuit.TickCount` which is
   already `RestoreTickCount`-able) and replayed.
2. **Testability.** The PBT and unit tests need to advance time deterministically.
   An injected clock (the host advances logical time, analogous to
   `InputHandle<T>.Push` before each `Step`) makes time a controllable input.
3. **Batch-oracle agreement.** For options C and B, whatever oracle is used must
   read the *same* clock the circuit used — only possible if the clock is an
   explicit value, not an ambient wall-clock.

So the clock is a **per-circuit logical-time input**, set before each `Step`,
monotone non-decreasing, persisted across snapshot — structurally the same shape
as the LATENESS `MutableFrontier` (`Core/Operators/Stateful/Frontier.cs`). In
fact under option B it likely *is* a frontier the runtime already knows how to
advance, persist, and combine.

## The compile seam (where `NOW()` is *not* a registry entry)

- **Parser.** `NOW` / `CURRENT_TIMESTAMP` / `CURRENT_DATE` parse to a dedicated
  node (`ResolvedNow`-style), not a `FunctionCallExpression` — they are
  keyword-spelled and (for `CURRENT_TIMESTAMP`) parenthesis-less. They must
  **not** route through `ScalarFunctionRegistry`; the registry's contract is
  purity, which `NOW()` violates.
- **Resolver.** Types the node (`TIMESTAMP` / `DATE`), and — for option B —
  enforces the temporal-filter position restriction (reject `NOW()` outside a
  sanctioned predicate).
- **Compiler.** Lowers the node to a read of the ambient clock cell (a closure
  over the per-tick logical-time value), *not* a per-row computation. For option
  B, lowers the qualifying predicate to the new time-driven operator instead of a
  plain `filter`.
- **`BatchPlanEvaluator`.** Gains a clock parameter so the oracle evaluates
  `NOW()` to the same value the circuit used.

## Recommendation

1. **Decide the meaning first** (the product call): frozen constant (C) or
   advancing temporal filters (B). Do **not** ship A as the default.
2. If a value is needed soon and frozen is acceptable, **C is the sound minimal
   step** — a constant, persisted at install, zero new operators, oracle intact.
   Document loudly that it does not advance.
3. The real feature is **B**, and it is a *project*: a time-driven operator, a
   clock-as-watermark unified with the LATENESS frontier, a redefined correctness
   oracle, persistence, and a restricted grammar. Scope it as its own effort with
   its own design pass — it is comparable in size to the LATENESS work, not to
   adding a scalar function.

## Non-goals

- **`RANDOM()` and other non-deterministic sources.** Same purity violation,
  worse (no monotone structure to exploit). Out of scope here.
- **Wall-clock `NOW()` read inside operators.** Explicitly rejected — breaks
  replay; see above.
- **`NOW()` as a general scalar in arbitrary positions** under option B — only
  the sanctioned temporal-filter predicates; everything else is a resolve error.

## Touch points (when implemented)

- `src/DbspNet.Sql/Parser/` — keyword(s) + dedicated AST node (not a function call).
- `src/DbspNet.Sql/Plan/Resolver.cs` — resolve the node; (B) position restriction.
- `src/DbspNet.Sql/Plan/ResolvedExpression.cs` — `ResolvedNow` node.
- `src/DbspNet.Sql/Expressions/` — lower to an ambient-clock read (bypasses
  `ScalarFunctionRegistry`).
- `src/DbspNet.Core/Circuit/` — per-tick logical-time input, set before `Step`,
  persisted (cf. `RootCircuit.TickCount` / the LATENESS `frontier.bin`).
- `src/DbspNet.Core/Operators/` — (B) the time-driven retraction operator.
- `src/DbspNet.Sql/Compiler/BatchPlanEvaluator.cs` — clock parameter for the oracle.
- `tests/.../EndToEnd/` — a redefined correctness oracle for time-dependent output.
