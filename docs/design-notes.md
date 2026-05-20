# DbspNet design notes

## What DBSP is (primer)

**DBSP** (Database Stream Processor) is a mathematical model for incremental
computation over relational data. The central objects are:

- **Z-sets.** A Z-set over a domain `K` is a finite function `K → Z` (the
  integers): a *weighted multiset*. A positive weight means "present this many
  times"; a negative weight means "retracted this many times". More generally
  the weights can live in any commutative ring; we call them **Z-ring
  values** below.
- **Streams.** A stream over a value type `T` is an infinite sequence `T₀, T₁,
  T₂, …`. In practice we only care about finite prefixes. A database view
  over time is a stream of Z-sets.
- **Circuits.** A circuit is a directed graph whose nodes are *operators* and
  whose edges carry streams. Operators consume one value per input stream per
  clock tick and emit one value per output stream. The whole circuit advances
  one tick per call to `step()`.
- **Integration `I` and differentiation `D`.** These are dual operators on
  streams. Given a stream of deltas, `I` produces the stream of running sums;
  given a stream of running sums, `D` produces the stream of deltas. They
  satisfy `D ∘ I = I ∘ D = id`.
- **Lifting `↑`.** Any pointwise function `f : T → T'` lifts to a stream
  function `↑f` that applies `f` at every tick.
- **Incrementalization.** The central theorem is that, for any query `Q`
  expressible as a composition of linear and bilinear operators on Z-sets,
  there exists an equivalent *incremental* circuit `Q^Δ` whose work at each
  tick is proportional to the size of the input delta, not the size of the
  cumulative state. For linear `Q`, `Q^Δ = ↑Q`. For bilinear `Q(a,b)`:
  `Q^Δ(Δa, Δb) = Q(Δa, b_prev) + Q(a_prev, Δb) + Q(Δa, Δb)` with `a_prev` and
  `b_prev` the previous integrated states. For non-linear operators (like
  `distinct`) we implement the `D(Q(I(·)))` sandwich directly or provide a
  bespoke incremental operator.

See Budiu et al., *DBSP: Automatic Incremental View Maintenance for Rich Query
Languages*, VLDB 2023, for the full theory.

## Implementation choices

### Weight algebra via `INumber<T>`

Feldera's runtime defines a trait hierarchy (`MonoidValue` → `GroupValue` →
`RingValue` → `ZRingValue`) because Rust had no standard numeric abstraction
at the time. .NET 10 already ships [`INumber<TSelf>`][INumber] in
`System.Numerics`, which is a commutative-ring-with-identity plus ordering
and formatting. Every built-in numeric (`int`, `long`, `double`, `decimal`,
`BigInteger`, `Int128`, …) implements it.

We therefore constrain Z-set weights as

```csharp
where TWeight : struct, INumber<TWeight>
```

- **`struct`** prevents interface-call boxing — static-abstract dispatch stays
  monomorphized only when `TWeight` is a value type known at JIT time.
- **`INumber<TWeight>`** provides `TWeight.Zero`, `TWeight.One`, and the
  `+ - * - ==` operators we need.

The canonical instance in v1 is `long`. Future weight types (e.g. a `bool`
set-indicator, a checked-int, a decimal multiplicity) can be added as
wrapper structs that implement `INumber<Self>`.

A bespoke `IZRing<T>` interface would be more pedagogically faithful to
Feldera's type system but would prevent using BCL primitives directly as
weights, which is too much ceremony for a prototype. The abstraction is
documented here; code uses `INumber<T>`.

[INumber]: https://learn.microsoft.com/en-us/dotnet/api/system.numerics.inumber-1

### Z-set zero-weight invariant

The most common source of subtle bugs in a Z-set implementation is *leaving a
zero-weight entry in the backing map*. `distinct` sees it as "the key is
present"; `aggregate` double-counts; joins emit phantom rows. We enforce the
invariant in a single central mutation path (`ZSet.Add`) and forbid operators
from mutating the backing dictionary directly.

### Row keys: `StructuralRow`

SQL rows are dynamic tuples: arity and column types vary per query. We need a
value-equality row type that (a) can be a `Dictionary<,>` key, (b) caches its
hash code (rows are immutable once built and hashed many times during join
probing), and (c) avoids the `ValueTuple` arity-7 cliff and the type-unsafety
of `object[]`.

Implementation: `StructuralRow` stores an `ImmutableArray<object?>` and
computes its hash once at construction. Element equality uses `object.Equals`
so value types box into the `ImmutableArray` — we accept the allocation cost
for prototype simplicity and document it as a `TODO` for a later pass
(candidate improvements: per-schema generated row structs, or a byte-buffer
encoding of SQL values).

### Direct incremental operators, not literal D-I sandwich

For bilinear operators (join, cartesian product) we implement the incremental
form directly:

```
output_t = Q(Δa_t, b_{t-1}) + Q(a_{t-1}, Δb_t) + Q(Δa_t, Δb_t)
a_t = a_{t-1} + Δa_t
b_t = b_{t-1} + Δb_t
```

This is equivalent to the literal `D(↑Q(I(·), I(·)))` sandwich by the
bilinearity theorem (Feldera/VLDB23 §4.3), but requires less state (two
integrated traces instead of four intermediate streams) and maps directly to
how Feldera's production operators are actually written. The sandwich form
is documented as a conceptual bridge, not an implementation strategy.

### Three-valued logic (NULL) end-to-end

SQL's `NULL` propagates in surprising ways (`NULL = NULL → NULL`; `NULL AND
FALSE → FALSE`). We encode NULL end-to-end: every SQL column value is a
nullable wrapper, arithmetic and comparison propagate NULL, boolean operators
follow the SQL truth tables. This adds complexity (most prototypes skip NULL)
but is a prerequisite for any query exercise that touches real data.
