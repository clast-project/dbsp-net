# Design: connector / adapter framework (input + output)

A general connector layer that feeds external data into a `CompiledQuery` and writes
its results out, with checkpoint/exactly-once alignment designed in from the start.
First backing implementation is [engineered-wood](https://github.com/CurtHagenlocher/engineered-wood)
(Arrow-native Parquet/Delta/Avro/Vortex/Lance); first target workload is ivm-bench
(TPC-DI over Delta, truncate-mode full-state output). Scoped 2026-07-16; design only.

## Decisions taken (this design honours all four)

1. **Framework first** — design the connector abstractions + exactly-once alignment up
   front, then wire ivm-bench onto them (not a minimal one-off).
2. **Schema: infer-unless-declared** — if a table is declared (`CREATE TABLE` /
   programmatic `Schema`), the declaration is authoritative and the source is
   validated/coerced onto it; if not declared, infer the `Schema` from the source's
   Arrow schema and `Catalog.Register` it.
3. **Abstractions + impls split** — `DbspNet.Connectors.Abstractions` (engine-only, no
   engineered-wood dependency) + `DbspNet.Connectors.EngineeredWood` (Delta/Parquet
   implementations). Other formats/engines can plug in either side.
4. **CDF-follow, one tick per version** — initial snapshot, then `ReadChangesAsync` per
   new Delta version; one engine `Step` per source version (Feldera `always` semantics).

## Reference-implementation grounding

The contract below is the intersection of two proven designs:

- **Feldera** (our engine class — DBSP, view-only SQL, Delta connectors): schema is
  declared in SQL and the connector *conforms* to it; input offset = **Delta version**;
  modes snapshot / follow(CDF) / snapshot_and_follow / cdc; `transaction_mode`
  (none/snapshot/catchup/always) maps source versions to engine transactions;
  **at-least-once** with primary-key idempotency (exactly-once not yet); output is
  changelog (`__feldera_op`/`__feldera_ts`) by default, or `send_snapshot` + `truncate`
  for full materialization.
- **Spark Structured Streaming**: `Source` = `latestOffset` + `getBatch(start,end)` +
  `commit(offset)`, offsets in a write-ahead **offset log** + **commit log**; `Sink` =
  `addBatch(batchId, data)` required **idempotent** (dedup by batchId) ⇒ exactly-once
  with replayable sources; output modes append / update (≈ our delta) vs **complete**
  (whole result table each batch ≈ our `CurrentView`/truncate).

Our design = Spark's offset-log/commit-log durability model + Feldera's version-as-offset
and transaction-mapping, specialised to DBSP where **the engine snapshot is the
checkpoint** and a **replayable source needs no input WAL** (re-read from its committed
version). This is the key simplification DBSP + Delta buy us.

## What already exists (build-on, don't rebuild)

- **Arrow bridge into the engine** — `DbspNet.Arrow.ArrowExtensions.PushArrow(TableInput,
  RecordBatch, ReadOnlySpan<long> weights)` and zero-copy `PushArrowZeroCopy`; delta-out
  `ToArrowDelta()`. Source/sink speak `RecordBatch`, so no bespoke row translation.
- **Programmatic catalog** — `Catalog.Register(string, Schema)` is public; DDL is just
  one front-end to it.
- **Arrow-aligned `SqlType`** — Utf8String/Date32/Time64/Timestamp/Decimal128 share
  Arrow's bit layout (`ArrowSchemaBridge` comment).
- **Materialized output** — `CompileOptions.StoredOutput` → `CurrentView` /
  `EnumerateView()` (the truncate-out seam, already shipped).
- **Engine checkpoint** — `Snapshot.WriteAsync/ReadAsync` keyed on `TickCount` +
  `LogicalTime`, plan/schema-fingerprint guarded; `WalRecorder` for input-replay
  durability; `ITableFileSystem` (shared with engineered-wood — same abstraction!).

## Gaps to fill in the engine core (small, general)

- **G1 — Arrow→`SqlType` reverse mapping.** Only `SqlType`→Arrow exists
  (`ArrowSchemaBridge.ToArrow`). Add `FromArrow(Apache.Arrow.Schema) : Schema` +
  `FromArrowType(IArrowType) : SqlType` in `DbspNet.Arrow`, with explicit handling of
  the lossy/ambiguous cases (timestamp unit/tz → µs `TIMESTAMP`; `Decimal128Type(p,s)` →
  `DECIMAL(p,s)`; dictionary → decoded value type; `LargeString`→VARCHAR; nested
  `Struct`/`List` → **reject or flatten** per the ROW-flattening precedent). Needed for
  schema inference *and* for validating a declared schema against a source.
- **G2 — `ToArrowView()`.** A materialized-view → `RecordBatch(es)` builder (reuses
  `ArrowColumns.Build`), the mirror of `ToArrowDelta()`, for truncate-mode sinks.
- **G3 — checkpoint-metadata hook. DONE (phase 2, 2026-07-16).** `Snapshot.WriteAsync`
  gained an overload taking `IReadOnlyDictionary<string,string>? metadata`, written into
  the manifest (before the `current.txt` rotation that makes the snapshot visible) and
  read back via `Snapshot.ReadMetadataAsync`. Additive/optional — no manifest schema-
  version bump, backward-compatible with existing snapshots. `SnapshotCheckpointStore`
  now stores the offsets JSON under the manifest key `connector.offsets` instead of a
  sidecar, so engine-tick T and the source offsets commit as one atomic unit and can
  never diverge (the exactly-once alignment invariant).
- **G4 (maybe) — tick alignment on restore.** `RestoreTickCount`/`RestoreLogicalTime`
  are internal (only `Snapshot.ReadAsync` sets them). The runner aligns to the tick the
  snapshot restores; it never sets the tick directly. No change needed if connectors
  checkpoint *through* `Snapshot` (via G3).

- **G5 — UTF-8 as the text contract (2026-10-31; Copy default SHIPPED, validation open).**
  The engine stores VARCHAR as UTF-8, but the Arrow ingest paths decoded every cell to a .NET
  `string` and re-encoded it; they now copy the bytes. See [UTF-8 as the input contract](#utf-8-as-the-input-contract-draft-2026-10-31).

---

## Architecture

```
            DbspNet.Connectors.Abstractions            DbspNet.Connectors.EngineeredWood
            ─────────────────────────────────          ─────────────────────────────────
  IInputConnector   ── offsets, delta batches ──►  DeltaInputConnector  (snapshot + CDF)
  IOutputConnector  ── view / changelog write ──►  DeltaOutputConnector (truncate | changelog)
  ISchemaMapper                                    ParquetInputConnector (bounded snapshot)
  ConnectorOffset / OffsetStore                    ArrowSchemaMapper (G1) ── uses DbspNet.Arrow
  PipelineRunner  ─────────────────────────────►   (drives Step, checkpoint, recovery)
       │
       └── DbspNet.Sql (Catalog, CompiledQuery, TableInput, CurrentView)
       └── DbspNet.Persistence (Snapshot + G3 offset hook)
```

`Abstractions` depends only on `DbspNet.Sql` / `.Core` / `.Persistence` / `Apache.Arrow`
(no engineered-wood). `EngineeredWood` depends on both `Abstractions` and the EW
projects (via git submodule + `ProjectReference`, until EW ships NuGet).

---

## The abstraction layer (`DbspNet.Connectors.Abstractions`)

### Offsets

```csharp
// An opaque, comparable, serializable position in a source. For Delta it wraps a
// version (long); for a file it wraps "read/not-read"; for Kafka a partition→offset map.
public interface IConnectorOffset : IComparable<IConnectorOffset>
{
    string Serialize();               // durable form (goes in the checkpoint manifest)
}

// Per-source cursor persisted atomically with the engine snapshot (G3).
public readonly record struct SourceCheckpoint(string SourceName, string Offset);
```

### Input connector

A **pull, replayable** source. It never pushes on its own thread; the runner asks it
for the next batch of changes after a given offset. Delta CDF makes "changes after
version V" a first-class, replayable operation — the reason we can drop the input WAL
for replayable sources.

```csharp
public interface IInputConnector : IAsyncDisposable
{
    string Name { get; }                              // logical table name in the Catalog

    // Schema handshake (infer-unless-declared): if `declared` is non-null the connector
    // validates/coerces the source onto it and returns it; if null it infers a Schema
    // from the source and returns that (the runner then Catalog.Register()s it).
    ValueTask<Schema> ResolveSchemaAsync(Schema? declared, CancellationToken ct);

    // The source's newest available offset (Delta latest version). null ⇒ nothing yet.
    ValueTask<IConnectorOffset?> LatestOffsetAsync(CancellationToken ct);

    IConnectorOffset ParseOffset(string serialized);  // rehydrate from a checkpoint
    IConnectorOffset InitialOffset { get; }            // "before any data" sentinel

    // One tick's worth of change: the rows added/removed to move `from` → the next
    // offset (exclusive→inclusive). For "one tick per version", `NextAsync` returns a
    // single version's CDF; the runner Steps once per returned batch. A bounded source
    // (Parquet) returns its whole snapshot once, then Completed.
    ValueTask<InputBatch?> NextAsync(IConnectorOffset from, CancellationToken ct);
}

// Arrow-native delta: rows + signed per-row weights (CDF insert=+1, delete=-1,
// update = -1 preimage & +1 postimage), tagged with the offset it advances to.
public sealed record InputBatch(
    RecordBatch Rows, long[] Weights, IConnectorOffset Offset, bool Completed);
```

The connector maps CDF `_change_type` → weight; the runner calls
`TableInput.PushArrow(batch.Rows, batch.Weights)` then `Step()`. **This is where "one
tick per version" lives** — `NextAsync` yields exactly one version's changes.

### Output connector

Two modes, both grounded in the references. Idempotency is the sink's job (Spark
`addBatch(batchId)`); we pass the **engine tick** as the batch id.

```csharp
public interface IOutputConnector : IAsyncDisposable
{
    string ViewName { get; }
    ValueTask BindSchemaAsync(Schema viewSchema, CancellationToken ct);  // from CompiledQuery.OutputSchema

    // Materialized / truncate: replace the sink's contents with the full current view.
    // Naturally idempotent (overwrite is), so exactly-once needs nothing extra — the
    // marquee ivm-bench path.
    ValueTask WriteViewAsync(RecordBatch view, long tick, CancellationToken ct);

    // Changelog: append the tick's delta as insert/delete rows (+ op/ts metadata),
    // Feldera-style. Idempotent via `tick` as the dedup key (see Delta txn below).
    ValueTask WriteDeltaAsync(RecordBatch delta, long[] weights, long tick, CancellationToken ct);

    OutputMode Mode { get; }   // Truncate | Changelog
}
```

`OutputMode.Truncate` reads `CompiledQuery.CurrentView` (needs `StoredOutput`) via the
new `ToArrowView()` (G2); `Changelog` reads `ToArrowDelta()`.

### Schema mapper

```csharp
public interface ISchemaMapper
{
    Schema     InferSchema(Apache.Arrow.Schema source);         // G1: Arrow → SqlType
    Apache.Arrow.Schema ToArrow(Schema declared);               // existing ArrowSchemaBridge
    // Validate a source against a declaration; produce a column-reorder/coercion plan
    // (ignore unused source columns; coerce compatible types; error on incompatibles).
    SchemaBinding Bind(Schema declared, Apache.Arrow.Schema source);
}
```

`SchemaBinding` records, per declared column, which source column feeds it and any
coercion — applied once, then every `InputBatch` is projected through it. This is how
"declared wins, source conforms" and "ignore unused columns" (Feldera) are realised.

### The pipeline runner

Owns the loop; this is where exactly-once is enforced.

```csharp
public sealed class PipelineRunner
{
    // Config: input connectors (→ tables), the compiled query (StoredOutput on if any
    // sink is Truncate), output connectors (→ views), a checkpoint store + cadence.
    public async Task RunAsync(CancellationToken ct) { /* poll → step → write → checkpoint */ }
}
```

Loop, per poll round:
1. For each source, ask `LatestOffsetAsync`. For each that advanced, pull `NextAsync`
   from its committed cursor — **one version = one InputBatch**.
2. Feed that batch (`PushArrow(rows, weights)`), advance the source's in-memory cursor,
   and `Step()` (one tick per version). Multiple sources with new versions ⇒ multiple
   ticks this round, deterministically ordered by (source name, version).
3. For each output connector, write the tick's result (`WriteViewAsync(view, tick)` or
   `WriteDeltaAsync`).
4. On the checkpoint cadence (every N ticks / T seconds), **atomically commit**: engine
   `Snapshot.WriteAsync` carrying the per-source offsets as manifest metadata (G3).

`DrainAsync` runs this synchronously. **`DrainPipelinedAsync(prefetch)` overlaps source
read + Arrow decode with engine compute**: a background reader task reads and decodes up
to `prefetch` versions ahead into a bounded channel (backpressured) while the engine
Steps + writes the current version. The engine stays single-threaded and processes
versions in the same round-robin order, so the result is **identical to `DrainAsync`** —
only the source-I/O + decode latency is hidden. `ToArrowView`/`PushArrow` are split so the
expensive Arrow *decode* (`DecodeArrowDeltas`) runs on the reader thread (touching only
immutable schema) and the cheap *push* stays on the engine thread — two versions never
merge into one tick. The reader tracks its own read-ahead cursor; checkpoints record the
*committed* cursor (post-Step), so read-ahead is transparent to exactly-once (a crash
re-reads uncommitted versions from the replayable source).

`DrainPipelinedAsync` also does **write-behind** (1-deep): each tick's output is
*materialised* on the engine thread (`ToArrowView`/`ToArrowDelta` snapshot the live
`CurrentView`/`Current` into a fresh Arrow batch before the next `Step`), and the actual
sink write runs on a background task while the engine Steps the next version — hiding the
sink I/O behind compute. Ordering is preserved by awaiting the previous write before
starting the next; **durability** by awaiting the pending write before each checkpoint
(so a committed tick's output is always flushed) and at end-of-drain. So both source I/O
(ahead) and sink I/O (behind) overlap engine compute, while the engine stays a single
serial `Step` loop.

**Recovery**: `Snapshot.ReadAsync` restores engine state to tick T *and* returns the
per-source offsets committed at T; the runner resumes each source from `offset + 1`.
Because engine tick and source offsets were one atomic commit, they can't diverge —
**at-least-once for free, exactly-once when the sink is idempotent** (truncate always is;
changelog via the Delta `txn` app-id/version below). No input WAL needed for replayable
sources; the existing `WalRecorder` remains available for non-replayable ones.

---

## The engineered-wood implementations (`DbspNet.Connectors.EngineeredWood`)

### `DeltaInputConnector`

- `ResolveSchemaAsync`: open via `DeltaTable.OpenAsync(fs)`; `CurrentSnapshot.ArrowSchema`
  → if `declared` null, `ISchemaMapper.InferSchema` (register); else `Bind` (validate/
  coerce). CDF metadata columns (`_change_type`/`_commit_version`/…) are stripped from
  the data schema.
- Offset = `DeltaVersionOffset(long)`; `LatestOffsetAsync` = `RefreshAsync()` +
  `CurrentSnapshot.Version` (or `TransactionLog.GetLatestVersionAsync`).
- `NextAsync(from)`: initial (from == InitialOffset & snapshot desired) →
  `ReadAllAsync()` as one or more insert-only batches at the snapshot version; then
  `ReadChangesAsync(from.Version+1, from.Version+1)` for the *next single version* →
  map `_change_type` to weights (insert +1, delete −1, update_preimage −1,
  update_postimage +1), project through the `SchemaBinding`, tag `Offset = that version`.
- Backfill (`snapshot_and_follow`) = snapshot at `V0` then CDF from `V0+1`.

### `DeltaOutputConnector`

- `WriteViewAsync(view, tick)`: `DeltaTable.WriteAsync([view], DeltaWriteMode.Overwrite)`
  — atomic truncate/replace; returns the new Delta version. Idempotent by construction.
  This is the ivm-bench path (`+stored: true`, `mode: truncate`). The
  `O(view)` full rewrite per tick is the cost the benchmark measures.
- `WriteDeltaAsync(delta, weights, tick)`: append rows tagged `__op` (i/d/u) +
  `__ts`, Feldera-style, via `WriteAsync(…, Append)`. For **idempotent** changelog,
  use Delta's `txn` action `(appId = pipeline id, version = tick)` so a replayed tick is
  deduped — *verify engineered-wood exposes this*; if not, changelog exactly-once is a
  follow-on and truncate (idempotent already) is the default.

### `ParquetInputConnector`

Bounded snapshot source: `ResolveSchemaAsync` from `ParquetFileReader.GetSchemaAsync`;
`NextAsync` returns the whole file once (`ReadAllAsync`, all weights +1) with
`Completed = true`. Useful for one-shot loads and tests.

### `ArrowSchemaMapper` (implements `ISchemaMapper`, wraps G1/G2)

The Arrow↔`SqlType` mapping and binding/coercion logic; the only place external type
ambiguity is resolved.

---

## Exactly-once — the crux of "framework first"

The invariant: **for every durable checkpoint, engine-tick T and each source's committed
offset were written in one atomic commit.** DBSP makes the rest fall out:

- **Replayable input** (Delta by version): recovery re-reads `ReadChangesAsync(offset+1,…)`.
  No per-tick input log — the source *is* the log. (Non-replayable sources fall back to
  the existing `WalRecorder`.)
- **Deterministic engine**: given the same input deltas in the same tick order, the
  circuit reproduces the same output — so re-processing after recovery is safe.
- **Idempotent sink**: truncate/Overwrite is inherently idempotent (same view → same
  contents). Changelog needs (appId, tick) dedup.

Ordering within a checkpoint (Spark's offset-log-before, commit-log-after, adapted):
`Snapshot.WriteAsync` is the single commit; the offsets in its manifest (G3) are the
"what has been consumed through tick T" record. A crash between two checkpoints replays
from the last committed (tick T, offsets) — at-least-once; exactly-once at the sink via
idempotency. This matches Feldera's stated guarantee and Spark's checkpoint model without
a second write-ahead log.

Multi-source consistency: each source has an independent offset; a "batch" that spans
several source tables becomes several ticks (one per version), all covered by the next
checkpoint. If a workload needs cross-table atomic batches, a `transaction_mode:
catchup` variant (coalesce a round into one tick) is a later option — out of scope for
the chosen one-tick-per-version default.

---

## Testing strategy

- **Abstractions in isolation**: an in-memory `FakeInputConnector` (scripted versions of
  ±1 Arrow batches) + `FakeOutputConnector` (captures writes). Drive `PipelineRunner`;
  assert the sink equals a batch re-computation; assert one tick per version.
- **Recovery / exactly-once**: run N ticks, checkpoint at tick k, kill, recover, finish;
  assert the final sink equals an uninterrupted run and that ticks k+1…N were reprocessed
  without duplication (truncate) — a crash-injection test per checkpoint boundary.
- **Schema mapper (G1)**: round-trip `SqlType`→Arrow→`SqlType` for every type; binding
  tests for reorder/ignore-unused/coercion/incompatible-error; the ambiguous Arrow cases
  (timestamp tz, dictionary, nested).
- **engineered-wood round-trip**: write a local Delta table, mutate it across versions
  (append/delete/update), read via `DeltaInputConnector`, assert the engine view equals
  the table's current contents at each version; write the view back via
  `DeltaOutputConnector` (Overwrite) and diff.
- **Mini end-to-end**: a 3–4 table TPC-DI-shaped slice (an SCD2 view + a join) over local
  Delta, feeding batches, truncate-out, checked against a batch oracle — the ivm-bench
  shape in miniature before the full harness.

## Build phases

1. **Abstractions + fakes + runner** (no EW): interfaces, `PipelineRunner`, offset model,
   in-memory fakes, the differential + recovery tests. Proves the framework. **DONE**
   (G1/G2 pulled forward; `InputBatch` streams `VersionBatch`es so a large version isn't
   double-buffered — one Arrow batch live at a time).
2. **Core gaps G1/G2/G3** in `DbspNet.Arrow` / `DbspNet.Persistence`. **DONE** (G1/G2 in
   phase 1; G3 = offsets in the snapshot manifest, phase 2).
3. **engineered-wood + `DeltaInputConnector`/`DeltaOutputConnector`/`Parquet`**,
   round-trip tests against local Delta. **DONE (2026-07-16).** Consumed as nuget.org
   packages (`EngineeredWood.DeltaLake.Table` + `EngineeredWood.Parquet` at 0.1.0, which
   pull DeltaLake/Expressions/Core transitively); originally a git submodule at
   `external/engineered-wood`, swapped to packages 2026-07-20 once the fixes shipped to
   nuget.org. Apache.Arrow unified to **23.0.0** across dbsp-net + EW (one `RecordBatch`
   type across the boundary). New project
   `DbspNet.Connectors.EngineeredWood`: `DeltaInputConnector` (CDF-from-0 follow, one tick
   per version, `_change_type`→signed weights), `DeltaOutputConnector`
   (`Overwrite`=truncate / append changelog), `ParquetInputConnector` (bounded). Round-trip
   tests against real local Delta tables (create+append+overwrite → engine view == source
   contents; truncate sink == view; incremental resume across drains).
4. **Mini end-to-end**, then wire the real ivm-bench harness (input/output adapters +
   the drain signal) — the last mile to a measured run.

## UTF-8 as the input contract (DRAFT 2026-10-31)

> **Status:** `StringDecoding.Copy` is the ingest default on all three Arrow paths, each of which
> takes an optional `StringDecoding? strings` (null = Copy); `PushArrowZeroCopy` is `[Obsolete]` in
> favour of `strings: StringDecoding.Alias`. Not yet done: UTF-8 validation (contract item 4) and
> moving the remaining benchmark generators to `Utf8String` (increment 2).

### Why

UTF-8 is the text encoding of everything this engine reads and writes — Arrow `Utf8`/`LargeUtf8`,
Parquet `BYTE_ARRAY (UTF8)`, Delta, JSON, Kafka payloads, the Postgres wire protocol in practice —
and it is what the engine stores: a VARCHAR value is a `Utf8String` (UTF-8 bytes) in every
operator, trace and snapshot. A source and a sink that *share* some other encoding are rare enough
that the engine should not be shaped around them. Feldera is the same: its SQL strings are Rust
`str`, UTF-8 by type invariant.

Any boundary that hands the engine .NET `string`s therefore pays to encode, and one that hands it
UTF-8 by way of a `string` pays twice. Measured (2026-10-31):

- **Nexmark harness** (`w1profile`, 1M events): pushing `Utf8String` cells instead of `string`s
  takes **~281 B/event off every query** and **11–39% off ns/event** (q0 −31%, q20 −39%, q4 −17%).
  Every earlier Nexmark-vs-Feldera ratio charged us for this; Feldera's generator never pays it.
  (`NEXMARK_STRINGS=utf8` is now the generator default, `018919b`.)
- **Flat-state probe** (docs/design-flat-state-probe.md §3.4, q20 10M): the engine runs **19–22%
  faster** on UTF-8 input; its GC pause at W=1 falls 764 → 155 ms.

### Where the engine transcoded (as of 2026-10-31, before the default changed)

| Entry point | String cells arrive as | What happens per VARCHAR cell |
|---|---|---|
| `TableInput.Push` / `Insert` / `Delete`, `TypedTableInput.Push` (`object?[]` rows) | `string` or `Utf8String` | `string` → `Utf8String.Of` (encode + allocate); `Utf8String` passes through untouched (`BoundaryEncoder`) |
| `PushArrow(batch[, weights])` — the connectors' path (`PipelineRunner`) | Arrow `Utf8` | **`StringDecoding.Transcode`: UTF-8 → `string` → UTF-8, two allocations, two transcodes** |
| `ShardedTableInput.PushArrow` — parallel programs (`ProgramRunner` via `RunnerProgram`, so ivm-bench at W>1) | Arrow `Utf8` | **`Transcode`** |
| `DecodeArrowDeltas` — decode off the engine thread, push later | Arrow `Utf8` | **`Transcode`** |
| `PushArrowZeroCopy` | Arrow `Utf8` | `Alias`: the cell is a slice of the batch's buffer |
| Snapshot restore | Arrow IPC | `Arena`: one copy per column block (b83c593) |
| Output: `ToArrowDelta` / `ToArrowView` | — | appends the UTF-8 bytes directly — already right |

So the production ingest path — every connector, and ivm-bench — is the worst row in the table.

### The contract

1. **VARCHAR crosses every engine boundary as UTF-8.** `Utf8String` (or an Arrow `Utf8` column) is
   the native form. A .NET `string` stays accepted on the `object?[]` path as a convenience for
   callers that hold one (tests, small embedders): it is encoded exactly once, at the boundary,
   which is today's behaviour — the change there is documentation, not code.
2. **Transcoding a non-UTF-8 source is the connector's job**, never the engine's: a CSV reader with
   a Latin-1 option converts to UTF-8 before it builds a batch.
3. **Ownership: by default the engine owns what it keeps.** A retained row must never keep an
   input buffer alive or dangle when the caller frees it. The three ways to get bytes in, in the
   order an API should offer them:

   | Mode | Cost per VARCHAR cell | Retention | Use |
   |---|---|---|---|
   | **`Copy` (new; the default)** | one exact-size `byte[]`, a memcpy, no transcode | each cell owns its bytes — identical to today's `Transcode` | every ingest path |
   | `Arena` | one copy per column block | a surviving row pins its block's arena | restore, bulk loads where rows live and die together |
   | `Alias` | none | a surviving row pins (or, if native-backed, dangles on) the caller's buffer | callers that hand the engine the buffer for good |

   `Copy` is `Transcode` with the round trip removed: the same allocation count minus one, the
   same retention, so it can replace `Transcode` as the default with no behavioural risk.
   `Arena` stays the restore mode; it is wrong for ingest that filters, where one surviving row
   pins every string of its block (the chunk-retention finding of
   design-state-representation §10.H). `Alias` stays opt-in.
4. **Validity: validate at ingest by default.** Nothing validates UTF-8 today, and several
   operators assume it (`LENGTH` counts code points, `UPPER`/`LOWER`, `LIKE`, the regex functions).
   Feldera cannot hold invalid UTF-8 at all (Rust `str`), so per the adopt-Feldera-assumptions
   policy the default is to reject an invalid cell with an error naming table and column.
   `System.Text.Unicode.Utf8.IsValid` is vectorised, so the check should cost little next to the
   copy; it gets an opt-out for sources that guarantee validity (our own Arrow builders) and is
   measured in increment 1 before it ships.

### API shape

```csharp
public enum StringDecoding { Copy, Arena, Alias, Transcode /* kept for A/B only */ }

// Arrow ingest: one optional parameter, default Copy. PushArrowZeroCopy becomes
// PushArrow(..., StringDecoding.Alias) and is marked [Obsolete].
public static void PushArrow(this TableInput input, RecordBatch batch,
    ReadOnlySpan<long> weights, StringDecoding strings = StringDecoding.Copy);
public static void PushArrow(this ShardedTableInput input, RecordBatch batch,
    ReadOnlySpan<long> weights, StringDecoding strings = StringDecoding.Copy);
public static List<(object?[] Values, long Weight)> DecodeArrowDeltas(this TableInput input,
    RecordBatch batch, ReadOnlySpan<long> weights, StringDecoding strings = StringDecoding.Copy);

// object?[] path: unchanged signature; documented as "VARCHAR cells are Utf8String
// (passed through) or string (encoded once)".
public void Push(IEnumerable<(object?[] Values, long Weight)> deltas);
```

`IInputConnector` needs no change — it already yields Arrow batches; `PipelineRunner` and
`ProgramRunner` pick up the new default by not passing the parameter. Output is unchanged:
`ToArrowDelta`/`ToArrowView` already write bytes, and `Current`/`EnumerateView` already return
`Utf8String` cells.

### Beyond this design

The flat-state probe (docs/design-flat-state-probe.md) points at the longer road: state that owns
its strings in compacting arenas would copy each value once into state and allocate no per-cell
object at all. That is a state-representation change, not an API one; `Copy` is the right default
until then and does not get in its way.

### Increments

1. **`StringDecoding.Copy` + default flip** on the three Arrow ingest paths, with the UTF-8
   validation behind its opt-out. Gate: ivm-bench SF=3 batch-1 drain and allocation at W=12,
   ≥5 alternating runs per arm, outputs identical; the validation cost reported separately.
2. **Harness inputs**: Nexmark done (`018919b`); move the fraud generator and any other
   `string`-emitting benchmark input to `Utf8String`, with a flag to reproduce old runs.
3. **Docs + deprecation**: the `object?[]` contract in `TableInput`'s XML docs; `PushArrowZeroCopy`
   `[Obsolete]` in favour of the parameter.

### Measured: Transcode vs Copy vs Arena on ivm-bench (2026-10-31)

`DBSPNET_INGEST_STRINGS=transcode|copy|arena` (`57cf4f8`) on the three Arrow ingest paths;
`scripts/measure-ingest-strings.sh`, ivm-bench SF=3, W=12, 5 alternating runs per arm, one at a
time. Live heap is the bytes surviving a full compacting GC right after batch 1's drain, before any
output view forms. Raw: `docs/ingest-strings-results-20261031.txt`.

| arm | batch-1 drain, median [range] | batch-1 allocation | live after batch 1 |
|---|--:|--:|--:|
| Transcode (today) | 16.75 s [15.14–17.83] | 43.29 GiB | 9.65 GB |
| Copy | 15.77 s [15.63–16.30] | 42.93 GiB | 9.65 GB |
| Arena | 15.58 s [15.45–16.27] | 42.75 GiB | **9.51 GB** |

Output row counts are identical in every run of every arm, batches 1–3.

- **On ivm-bench, string ingest is a small term.** Removing the round trip saves 0.36 GiB (Copy)
  and 0.54 GiB (Arena) of 43 GiB — about 1%. Batch 1's allocation is engine-internal churn, as the
  flat-state probe found for q20. (The Nexmark harness gained far more, 11–39%, because its rows are
  narrow and string-heavy relative to the work done on them.)
- **Drain is ~1 s faster for both** (6–7% at the medians), but Transcode's runs spread over 2.7 s
  and overlap the others, so read it as "probably a little faster, certainly not slower".
- **Arena pins nothing measurable here — it keeps *less* live** (−0.14 GB): almost every ingested
  row survives into state, so the per-cell `byte[]` headers Copy pays outweigh the dead bytes an
  arena block holds.

**Recommendation.** Make **Copy** the ingest default: it is never worse than Transcode, and its
retention is exactly today's. Offer **Arena** as an opt-in for bulk loads where nearly everything
survives — ivm-bench's initial load is one, and wins slightly on every measure. Arena is not a safe
*default*: on a long-running stream behind a selective filter, one surviving row per block pins the
whole block, so memory would grow with the input rather than with the state. Copy-on-retain (alias
for the tick, copy where an operator keeps a row) is the design that gets Arena's allocation without
that risk, but at ~1% of ivm-bench allocation it is not worth building for this workload.

### Open questions

- **Validation scope.** Validate in `Copy` only (where we touch the bytes anyway), or in every mode?
  (Lean: every mode, opt-out per call — an `Alias` caller is the likeliest to have skipped it.)
- **`LargeUtf8` / `Utf8View`.** Arrow's 64-bit-offset and view string types should ingest the same
  way; check what the readers we use (engineered-wood, Arrow IPC) produce before increment 1.
- **Is `Transcode` worth keeping at all** once `Copy` lands? (Lean: keep one release as the A/B arm,
  then delete.)

## Change-site summary

| Site | Project | ~LOC |
|---|---|---|
| `IInputConnector`/`IOutputConnector`/`ISchemaMapper`/offsets | Connectors.Abstractions (new) | 200–300 |
| `PipelineRunner` + checkpoint/recovery | Connectors.Abstractions | 250–350 |
| In-memory fakes + tests | test project | 300–500 |
| `FromArrow`/`FromArrowType` (G1) + `ToArrowView` (G2) | DbspNet.Arrow | 150–250 |
| Snapshot manifest offset hook (G3) | DbspNet.Persistence | 60–100 |
| `DeltaInputConnector`/`DeltaOutputConnector`/`ParquetInputConnector` | Connectors.EngineeredWood (new) | 400–600 |
| `ArrowSchemaMapper` (binding/coercion) | Connectors.EngineeredWood | 150–250 |
| Delta round-trip + mini-e2e tests | test project | 400–600 |

## Open sub-decisions (not blocking the design; flag for build time)

- **G3 shape**: manifest string-map vs a typed checkpoint-contributor interface. (Lean:
  typed contributor, so the offset store is a first-class, testable seam.)
- **Nested/unsupported Arrow types on infer**: reject vs flatten (reuse the ROW-flatten
  precedent). (Lean: reject with a clear error in v1; flatten later.)
- **Changelog exactly-once** depends on whether engineered-wood exposes Delta's `txn`
  idempotent-write action; truncate needs nothing. (Verify during phase 3.)
- **Poll cadence / backpressure** for the runner (fixed interval vs event-driven);
  irrelevant to correctness, tune later.
