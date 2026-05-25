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

### Row keys: `StructuralRow` and the typed fast path

SQL rows are dynamic tuples: arity and column types vary per query. We need a
value-equality row type that (a) can be a `Dictionary<,>` key, (b) caches its
hash code (rows are immutable once built and hashed many times during join
probing), and (c) avoids the `ValueTuple` arity-7 cliff and the type-unsafety
of `object[]`.

The baseline implementation is `StructuralRow`: an `ImmutableArray<object?>`
with a hash computed once at construction. Element equality uses
`object.Equals`, so value types box into the array. Two compiler-side
optimisations sit on top of this baseline:

- **Emitted equality codec.** `EmittedEqualityCodec` replaces the default
  `StructuralRow` with a per-schema emitted subclass — same `object?[]`
  storage, but `Equals`/`GetHashCode`/`ToString` are generated against the
  exact column count and types, removing the per-element runtime dispatch.
  Selectable via `PlanToCircuit.Compile(plan, codec)`.

- **Typed-row pipeline.** `TypedPlanCompiler` builds an alternate circuit
  whose streams carry per-schema emitted *structs* (from `TypedRowEmitter`)
  rather than `StructuralRow` — no `object?[]`, no boxing on the hot path.
  `PlanToCircuit.Compile` tries this path first and falls back to the
  structural compile if any subexpression is outside scope; the two paths
  share snapshot codecs through a typed↔structural adapter so on-disk
  format stays compatible. The structural fallback is the only path that
  honours an explicit `IRowCodec<StructuralRow>`.

### Spine: LSM-style traces

The default `Trace`/`IndexedTrace` are flat dictionaries — fast in memory,
but they re-merge on every `Integrate` and have no natural disk
representation. `DbspNet.Core.Operators.Stateful.Spine` adds a sibling
hierarchy that keeps the running integral as a tiered sequence of
immutable sorted-columnar `SpineBatch`es with explicit compaction
(`ICompactionStrategy`, `TieredCompactionStrategy` as the default).
Probes pay a cache-line bloom check per batch before binary search;
`Integrate` is O(|delta| log |delta|) for the sort, no merge-on-write.
Every stateful operator has a spine-backed variant
(`SpineDistinctOp`, `SpineIncrementalAggregateOp`,
`SpineIncrementalJoinOp`, `SpineIncrementalLeftJoinOp`) exposed via
`SpineStatefulOperators` extension methods.

Two payoffs the flat trace can't easily match:

- **Per-batch snapshot.** Each batch is independently
  Arrow-IPC-serialisable, so snapshotting a spine is "write each batch
  to its own blob plus a manifest" rather than serialising a
  monolithic dictionary. Implemented via `SpineSnapshot` against the
  same `IZSetTraceCodec` / `IIndexedZSetTraceCodec` interfaces the
  flat path uses.
- **Disk spill.** `SpineSpillConfig` lets levels at or below a
  configurable threshold stay resident while deeper batches serialise
  through an `ITableFileSystem`. The per-batch bloom filter stays in
  memory, so most probes never touch disk.

The SQL compiler emits the flat operator family by default and the
spine family when `PlanToCircuit.Compile` is given
`CompileOptions { TraceFamily = TraceFamily.Spine }`. The spine traces
sort their keys and values, which the flat dictionary traces don't —
so the structural compile supplies a `StructuralRowComparer` (a total
order over `StructuralRow`: lexicographic, NULL-first, element compare
via the same non-generic `IComparable` the MIN/MAX path uses). The
typed-row fast path still emits the flat family, so a spine-mode query
compiles structurally; extending the typed pipeline to the spine is
the remaining step. See `docs/persistence.md` for the snapshot story.

### Arrow boundary

`DbspNet.Arrow` exposes `CompiledQuery.ToArrowDelta()` and
`TableInput.PushArrow(RecordBatch[, weights])` so DbspNet sits inside
Arrow-native pipelines without a `string` round-trip. Conversion is
column-major — type dispatch is hoisted out of the per-row loop into one
typed loop per column. The Z-set weight rides on the wire as a trailing
`__weight : Int64` column; readers that don't expect weights see a
well-formed Arrow stream and can ignore it. Decimal128 mantissas memcpy
directly between Arrow's 16-byte buffer and `Int128` via `MemoryMarshal`
— same little-endian layout, no per-cell shuffling. `Utf8String` /
`Decimal128` / `Date32` / `Time64` / `Timestamp` were chosen with this
boundary in mind (the SQL type system was pre-aligned to Arrow rather
than the reverse). Opt-in `PushArrowZeroCopy` aliases Arrow buffers
without copying, leaving lifetime management to the caller.

### Decimal128 and Utf8String

DECIMAL is `Clast.DatabaseDecimal.Decimal128` — an Arrow-aligned
`Int128` mantissa with scale carried in the type. Native arithmetic
kernels (banker's rounding for division), scale-aware comparison,
mixed-type promotion, and per-operator result-type rules following
SQL Server / Substrait semantics. SUM/AVG widen to `Int256` so
per-row `mantissa × weight` cannot silently wrap; the output narrows
to `Decimal128` with an explicit overflow check.

VARCHAR is `Utf8String` — an Arrow-aligned
`ReadOnlyMemory<byte>` with native UTF-8 equality, ordering, hashing
(XxHash3), code-point `LENGTH`, byte-wise `CONCAT`, and `Rune`-based
invariant `UPPER`/`LOWER`. No `string` round-trip on the SQL hot path.

### Persistence

`DbspNet.Persistence` ships approaches A (input WAL), B (end-of-tick
snapshot), and C (snapshot + WAL hybrid) — see `docs/persistence.md`
for the full design discussion. The interesting choices:

- **Cloud-shaped storage primitive.** Everything goes through
  `IBlobStore`, an S3/GCS/Azure-shaped abstraction with one durability
  guarantee: atomic single-blob write. Multi-blob commits are
  encoded as "write all the new blobs first, then write a pointer
  blob (`current.txt`) last" — the same pattern that works on object
  stores where directory rename doesn't exist.
- **No serialised aggregator caches.** `IncrementalAggregateOp`'s
  per-group `_aggCache` / `_stateCache` are rebuilt on `Load` by
  walking the restored trace and replaying through
  `aggregator.Update`, rather than serialised separately. This is
  what the original (B) sketch flagged as needing
  `SqlAggregator.SaveState/LoadState` per subclass; rebuilding side-
  steps the codec proliferation.
- **Schema drift detection.** Snapshot manifests carry both a plan
  fingerprint (operator-type sequence, with generic args) and a
  schema fingerprint (per-op column names + `SqlType.Display`) so
  VARCHAR-length / DECIMAL-precision / NULL-NOT-NULL drift surfaces
  on `Load` instead of corrupting silently.

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
