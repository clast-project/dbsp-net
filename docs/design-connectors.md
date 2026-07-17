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
3. **engineered-wood submodule + `DeltaInputConnector`/`DeltaOutputConnector`/`Parquet`**,
   round-trip tests against local Delta. **DONE (2026-07-16).** Submodule at
   `external/engineered-wood` (build-isolated via `external/Directory.Packages.props`
   disabling CPM + `external/Directory.Build.props`); Apache.Arrow unified to **23.0.0**
   across dbsp-net + EW (one `RecordBatch` type across the boundary). New project
   `DbspNet.Connectors.EngineeredWood`: `DeltaInputConnector` (CDF-from-0 follow, one tick
   per version, `_change_type`→signed weights), `DeltaOutputConnector`
   (`Overwrite`=truncate / append changelog), `ParquetInputConnector` (bounded). Round-trip
   tests against real local Delta tables (create+append+overwrite → engine view == source
   contents; truncate sink == view; incremental resume across drains).
4. **Mini end-to-end**, then wire the real ivm-bench harness (input/output adapters +
   the drain signal) — the last mile to a measured run.

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
