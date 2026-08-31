---
name: connector-framework
description: ACTIVE arc — input/output connector (adapter) framework for DbspNet; design in docs/design-connectors.md
metadata: 
  node_type: memory
  type: project
  originSessionId: d04e1240-137c-49b7-b437-e6290184b5c5
---

Building a general input/output connector layer for DbspNet (feed external data into a
CompiledQuery, write results out), driven by ivm-bench's need for Delta I/O. Design lives
in `docs/design-connectors.md` (source of truth). Started 2026-07-16.

**Data-access lib:** [engineered-wood](https://github.com/CurtHagenlocher/engineered-wood),
local at `C:\src\GitHub\engineered-wood` (Curt's). Arrow-`RecordBatch`-native; read+write
for Parquet/Delta/Avro/Vortex/Lance; net10.0/net8.0/netstandard2.0; Apache.Arrow 23.0.0.
NOT yet on NuGet → consume via git submodule + ProjectReference (interim). Key APIs:
`DeltaTable.OpenAsync/CreateAsync`, `ReadAllAsync`, `ReadAtVersionAsync`,
**`ReadChangesAsync(startVer,endVer)`** (CDF: `_change_type` insert/delete/update_pre/post
+ `_commit_version`; INFERS changes from Add/RemoveFile even without CDC files),
`WriteAsync(batches, DeltaWriteMode.Append|Overwrite)` → new version (long); Overwrite =
atomic truncate. I/O behind `ITableFileSystem` (SAME abstraction DbspNet.Persistence uses).

**Four decisions Curt made (2026-07-16), all the more-general option:**
1. Framework first (design abstractions + exactly-once up front, not a minimal one-off).
2. Schema: infer-unless-declared (declared Schema wins + source validated/coerced onto it;
   else infer from source Arrow schema and Catalog.Register).
3. Abstractions+impls split: `DbspNet.Connectors.Abstractions` (no EW dep) +
   `DbspNet.Connectors.EngineeredWood`.
4. CDF-follow, one engine tick per Delta version (Feldera "always" semantics).

**Reference grounding (not re-derive):** Feldera = schema declared in SQL, offset=Delta
version, at-least-once + PK idempotency, output changelog-or-truncate. Spark Structured
Streaming = Source(latestOffset/getBatch/commit + offset-log/commit-log), Sink(addBatch
(batchId) idempotent), output modes append/update(≈delta) vs complete(≈CurrentView/
truncate). Our design = Spark durability model + Feldera version-as-offset, specialized to
DBSP where **the engine Snapshot IS the checkpoint** and a **replayable source needs NO
input WAL** (re-read from committed version). ivm-bench uses the truncate full-state path =
our CurrentView (naturally idempotent → exactly-once free there).

**Already exists (build-on):** DbspNet.Arrow bridges RecordBatch→engine (`PushArrow(batch,
weights)`, zero-copy `PushArrowZeroCopy`, `ToArrowDelta`); `Catalog.Register(name,Schema)`
public; SqlType is Arrow-bit-aligned; StoredOutput→CurrentView/EnumerateView shipped;
Snapshot keyed on TickCount+LogicalTime + ITableFileSystem.

**Three core gaps the framework needs (all small/general):**
- G1: Arrow→SqlType reverse map (only SqlType→Arrow exists in `ArrowSchemaBridge.ToArrow`);
  handle timestamp unit/tz, Decimal128(p,s), dictionary-decode, LargeString, nested reject/
  flatten. In DbspNet.Arrow.
- G2: `ToArrowView()` (mirror of ToArrowDelta, reuses ArrowColumns.Build) for truncate sinks.
- G3: checkpoint-metadata hook so a connector persists per-source offsets ATOMICALLY with
  Snapshot.WriteAsync (manifest metadata) → engine-tick T and source-offset V can't diverge.
  The one core change exactly-once genuinely needs. (RestoreTickCount is internal → connectors
  checkpoint THROUGH Snapshot, never set tick directly.)

**Build phases:** (1) Abstractions + in-memory fakes + PipelineRunner + differential/recovery
tests (no EW). (2) Core gaps G1/G2/G3. (3) EW submodule + Delta/Parquet connectors + round-
trip tests. (4) Mini TPC-DI e2e → wire real ivm-bench harness.

**PHASE 1 DONE + pushed (2026-07-16, commit 949fe6b).** New project
`DbspNet.Connectors.Abstractions` (no EW dep): IInputConnector (pull/replayable,
NextAsync = one version's Arrow rows+signed weights = one tick), IOutputConnector
(Truncate/Changelog), ISchemaMapper + ArrowSchemaMapper (infer-unless-declared,
name-matched bind w/ unused-column drop), IConnectorOffset/LongOffset, ICheckpointStore
+ SnapshotCheckpointStore (engine Snapshot + offsets.json sidecar), PipelineRunner
(CreateAsync wires schema→register→compile→bind; DrainAsync = poll→NextAsync→PushArrow→
Step→write→checkpoint one tick/version; RestoreAsync restores engine to tick T + resumes
each source from committed offset). **G1 (Arrow→SqlType FromArrow/FromArrowType) + G2
(ToArrowView) pulled forward into DbspNet.Arrow** (needed to test real schema-infer +
truncate paths). Tests: differential view≡batch (filter/SUM/COUNT/DISTINCT/rank), one-
tick-per-version, changelog cadence, schema infer/bind/reject, full checkpoint+recovery.
Suite 1967. **Deviations from design doc:** SchemaBinding is applied INSIDE the connector
(it owns its source), not by the runner. **G3 still deferred** (offsets sidecar written
back-to-back w/ snapshot = at-least-once; torn checkpoint falls back to prior).

**PHASE 2 DONE + pushed (2026-07-16, commit 77b35dd).** G3 hardened: SnapshotManifest got
an optional Metadata (string→string) section (ADDITIVE, no schema-version bump, back-compat
w/ existing snapshots); Snapshot.WriteAsync metadata overload writes it into manifest BEFORE
current.txt rotation; Snapshot.ReadMetadataAsync reads it back. SnapshotCheckpointStore now
stores offsets JSON under manifest key `connector.offsets` (no more offsets.json sidecar) →
engine-tick T + source offsets are ONE atomic commit, can't diverge. Tests: offsets-in-
manifest/no-sidecar/round-trip. Suite 1969.

**PHASE 3 DONE + pushed (2026-07-16, commit 03d977d).** EW added as git submodule at
`external/engineered-wood`. **Build integration gotchas (carry these):** (1) dbsp-net uses
Central Package Management (Directory.Packages.props, CPM on) + strict analyzers/
TreatWarningsAsErrors in root Directory.Build.props; EW has NO Directory.Packages.props →
its projects would walk up to dbsp-net's CPM and fail NU1008. FIX: `external/Directory.Packages.props`
(ManagePackageVersionsCentrally=false) + `external/Directory.Build.props` (empty barrier)
isolate the submodule. EW's own `src/Directory.Build.props` already stops the Build.props
walk. (2) Apache.Arrow version: dbsp-net was 21.0.0, EW pins 23.0.0; RecordBatch crosses
the boundary so MUST unify → bumped dbsp-net Directory.Packages.props to 23.0.0, full suite
re-validated green. New project DbspNet.Connectors.EngineeredWood: DeltaInputConnector
(ReadChangesAsync CDF-from-0, one tick/version, _change_type→signed weights via shared
ArrowProjection; EW infers CDF from Add/RemoveFile even w/o CDF table property; offset=Delta
version; replayable), DeltaOutputConnector (Overwrite=truncate idempotent / append changelog
w/ __op+__ts), ParquetInputConnector (bounded). **Framework refinement:** InputBatch now
STREAMS VersionBatches (IAsyncEnumerable) not a materialized list — a large version isn't
double-buffered (one Arrow batch live at a time; PushArrow copies each out). Runner/fake
updated. Tests: real local Delta create+append+overwrite → engine view==source contents
(delete inference + Z-set cancellation) / truncate sink==view / incremental resume. Suite
1971. EW namespaces: EngineeredWood.DeltaLake.Table (DeltaTable/DeltaWriteMode), .IO
(ITableFileSystem — DISTINCT from DbspNet's), .IO.Local (LocalTableFileSystem), .DeltaLake.
ChangeDataFeed (CdfConfig). Delta APIs: OpenAsync/CreateAsync/OpenOrCreateAsync, RefreshAsync,
CurrentSnapshot.Version/.ArrowSchema, ReadChangesAsync(start,end), ReadAllAsync, WriteAsync
(batches, Append|Overwrite)→version.

**PHASE 4 mini-e2e DONE + pushed (2026-07-16, commit 875dfa2).** `MiniPipelineE2ETests`:
two Delta sources (SCD2 `company` multi-row-per-key + `trade` facts) → view = rank-in-output
current-record (ROW_NUMBER()=1 in CASE, filtered) JOIN facts GROUP BY SUM → truncate Delta
sink; multi-source drain (one tick/version, round-robin), engine view + sink == batch oracle
(+ concrete expected). The exact SCD2-current-join-aggregate shape that gated ivm-bench, now
100% connector-driven. Suite 1972. (Note: use rank-in-output CASE form, NOT the rn<=k TOP-K
filter — BatchPlanEvaluator has a BatchPartitionedRank arm but NO PartitionedTopKPlan arm, so
TOP-K views can't be batch-oracle-compared.)

**IVM-BENCH PARTICIPATION — Phase A DONE + pushed (2026-07-16, commits bd6a6f0 + 7fb10a1).**
Local ivm-bench checkout at `D:\src\ivm-bench` (remote mdrakiburrahman/ivm-bench; Curt can PR).
Decisions: **Phase A first (in-repo program runner), defer dbt-integration choice to Phase C.**
Built the MULTI-VIEW PROGRAM engine capability (was missing — compiler only did single query):
`PlanToCircuit.CompileProgram(tables, views)` folds CREATE VIEWs in dep order into ONE circuit,
views = SHARED streams (pre-populate the scan env name→stream so `FROM <view>` wires to the
built stream), each output view wrapped in Integrate → `CompiledProgram`/`ProgramOutput`.
`SqlProgram.Resolve/Compile` resolves statements registering each view's schema in the catalog
(resolver treats view like a table). `ProgramRunner` (Connectors.Abstractions): N DeltaInput→
tables (declared schema validated), M DeltaOutput→output views, per-batch DrainAsync+WriteOutputs
(truncate). Structural-path-only; Phase A omits cross-view LATENESS GC (sound/unbounded). Tests:
program compiler vs dep-order batch oracle + Delta e2e (2 sources→shared SCD2 current view→2
truncate sinks, across full-load+append batches). Suite 1982.

**ivm-bench harness contract (from exploration, non-derivable — for Phase B/C):** Docker+Python+dbt,
BESPOKE per engine (no plugin API; if/elif on engine name across benchmark-server + dbt-server).
Feldera=reference (our class): a `pipeline-manager` SERVER (prebuilt image, REST: PUT /v0/pipelines/
{name}, start/pause/resume, /stats) + harness glue: (1) dbt project `dbt-projects/feldera/` (70
models, ALL Delta connectors as STATIC YAML in dbt_project.yml `+connectors`: delta_table_input
mode snapshot/snapshot_and_follow, delta_table_output mode:truncate, gold `+stored:true`); (2)
external `dbt-feldera` adapter (turns models→CREATE TABLE-with-connectors + CREATE VIEW, deploys,
compiles to ONE circuit); (3) dbt-server `feldera_client.py`+`handlers/feldera.py` = pause→resume
(TIMER START)→poll /stats for DRAIN (quiescence: signature stable FELDERA_QUIESCENCE_S=120s +
total_processed>=total_input + no tx-in-flight), writes run-feldera-batch{N}.json; (4) registry
edits (models/config.py ENGINE_PORTS/COMPOSE_FILES/MAIN_SERVICES, engine_runner.py dispatch,
chart.py, experiments/*.json) + docker-compose.benchmark.feldera.yml (mounts mount/raw/<SF>/delta
+ mount/results/<SF>/<engine>). 3 batches: full-load→append→append. Full add-engine checklist +
file:line refs captured during exploration.

**Phase B DONE + pushed (2026-07-16, commit 2f48064).** New `DbspNet.Server` (ASP.NET minimal
API, web SDK) = DbspNet's pipeline-manager analogue. `DbspNetEngine`: DeployAsync(ProgramSpec:
SQL DAG + Delta in/out bindings)→SqlProgram.Compile + wire DeltaInput/Output via ProgramRunner;
Resume()=start background RunBatchAsync (timer start, cursors persist across batches so resume
picks up only new versions); WaitAsync()=block until batch drained+outputs truncate-written
(timer stop)→duration+per-output row counts (status success). HTTP: /healthz /deploy /resume
/wait /pause /stats on :8080. Dockerfile (multi-stage, build from REPO ROOT so EW submodule
present; needs `git submodule update --init`) + .dockerignore. Input Mode field ignored (CDF-
follow subsumes snapshot/snapshot_and_follow). Tests: engine core deploy→resume→wait ×2 batches
over real Delta vs oracle; HTTP host smoke-tested (healthz/stats OK). Suite 1983. Pause=no-op
(our batch drains to completion, unlike Feldera's continuous stream).

**Phase C IN PROGRESS (2026-07-16). DECIDED: compile-only BYPASS (no custom dbt adapter) —
much less work, timing-neutral (deploy/compile excluded from measured batch, like Feldera's
Rust compile).** ivm-bench fork = CurtHagenlocher/ivm-bench (D:\src\ivm-bench), work on branch
`dbspnet-engine`. **MAJOR MILESTONE: the full 70-model ivm-bench DAG compiles as ONE DbspNet
program → /compile {ok:true, outputViews:18}.** Built the translator `src/containers/dbt-server/
services/dbt_to_program.py` (committed on branch): reads the feldera dbt models + resolves dbt
ref/source + the one dbt_utils.generate_surrogate_key macro (only jinja used, no {%%} statements
or config blocks) + topo-sorts → CREATE TABLE (sources, col DDL) + CREATE VIEW AS. POSTs to the
DbspNet service /compile (new dry-run endpoint added to DbspNet.Server this phase). **Two real
engine gaps surfaced + FIXED (dbsp-net commit 6d3bee8, suite 1985):** (1) numeric<->string
coercion threaded through SqlProgram.Compile/Resolve (numericStringCoercion flag; service always
on); (2) CAST(DOUBLE/REAL AS DECIMAL) added (DecimalRuntime.FromDouble + BuildCastToDecimal float
branch). New DbspNetEngine.Compile + POST /compile + CompileSpec/CompileResult.

**Phase C harness DRAFT DONE + pushed (2026-07-16, ivm-bench fork branch `dbspnet-engine`,
commit 50a407b — ready for PR to mdrakiburrahman/ivm-bench).** Full changeset (84 files):
dbt-projects/dbspnet/ (copy of feldera, 70 models, output uris→/data/processed/dbspnet, adapter
profile dropped); services/dbt_to_program.py (translator, extended to extract Delta +connectors
bindings from dbt_project.yml → 20 inputs/16 output_bindings) + dbspnet_client.py + handlers/
dbspnet.py (deploy→resume→wait; py_compile-clean); docker-compose.benchmark.dbspnet.yml (dbspnet-
server + dbt-server); **build follows the repo's from-source pattern (like duckdb-openivm): new
`src/containers/dbspnet/Dockerfile` git-clones clast-project/dbsp-net at a pinned DBSPNET_COMMIT
(=6d3bee8) + engineered-wood submodule + publishes DbspNet.Server — NO sibling checkout needed
(commit d2de278; Curt correctly flagged the sibling shortcut). Feldera = prebuilt image, from-
source engines clone-at-pinned-commit in their Dockerfile w/ REPO+COMMIT ARGs.**
benchmark-server registries (config.py port 5009/compose/main-service; engine_runner.py dispatch
+ _run_dbspnet_batch1/_wait — dbspnet uses GENERIC append for batches 2/3, NOT feldera's pause-
append-resume, since our batch drains-then-idles; chart.py/oat_chart.py order+colour + _collect_
feldera_status generalized(engine) for DBSP per-model success); experiments/dbspnet.json (dbspnet
vs feldera SF=3); DBSPNET.md (design+run+status). **VALIDATED (no Docker): full 70-model DAG →
/compile ok=true 18 outputs; deploy→resume→wait over local Delta vs oracle.**

**NOT YET DONE — needs Docker/WSL (Phase D, Curt's env): actual 3-batch benchmark over TPC-DI
data (spark-batch-loader datagen), the compose build of dbspnet-server from sibling repo, cross-
container wiring under a real OAT run.** Known follow-ons in DBSPNET.md: 2 gold views (fact_market
_history, daily_market_pulse) are +stored-but-no-output-connector = compute-but-don't-write; draft
only materializes views WITH an output binding (integrate-only outputs = follow-on); per-view
timings report whole-batch duration. Engine side FULLY PROVEN in-repo.

**Deferred follow-ons PRIORITIZED (2026-07-16):** Tier 1 (correctness) = bag multiplicity in
truncate sink — **DONE + pushed (commit dcf8320): ToArrowView now expands each row by its
weight** (row of weight w emitted w times, returned weights all 1; ToArrowDelta untouched);
DeltaOutputConnector round-trips bags; test = UNION ALL view id-weight-2 → 4 physical sink
rows → re-collapse to view. Suite 1973. Remaining tiers: Tier 2 (do WITH real harness — they
hinge on actual source schemas) = nested/struct Arrow types on infer (flatten vs reject),
coercion beyond nullability in Bind (only bites declared schemas w/ type drift; ivm-bench
infers), chunk large initial version into multiple ticks (semantic — breaks one-tick/version,
needs Curt's OK + scale biting). **Read-ahead pipelining DONE + pushed (2026-07-16, commit ab812c6)** — Curt asked if the
next batch is read in parallel with processing the previous; it WASN'T (DrainAsync fully
sequential). Added `PipelineRunner.DrainPipelinedAsync(prefetch)`: background reader task
reads+decodes up to `prefetch` versions ahead into a bounded (backpressured) channel while
the engine Steps+writes the current one. Engine stays single-threaded, same round-robin
order → byte-identical to DrainAsync. KEY correctness split: `ArrowExtensions.DecodeArrowDeltas`
does the expensive per-column Arrow extract on the READER thread (touches only immutable
schema); the cheap `Push` stays on the engine thread so two versions never merge into one
tick. Reader has its own read-ahead cursor; checkpoints record the COMMITTED cursor (post-
Step) → read-ahead transparent to exactly-once. Tests: pipelined==sequential==batch + a
deterministic overlap test (gated slow sink stalls driver, reader NextCount≥2 w/ 0 writes).
Suite 1978. **Write-behind DONE too (commit be1a41e):** DrainPipelinedAsync now overlaps
BOTH ends — 1-deep write-behind: each tick's output materialized on engine thread (ToArrowView/
ToArrowDelta snapshot live CurrentView before next Step) + actual sink write on a background
Task while engine Steps next; ordering=await-previous-before-next, durability=await-pending-
before-checkpoint + at end. Engine stays single serial Step loop. Test: Delta truncate round-
trip via DrainPipelinedAsync + checkpoint-every-tick over real Delta. Suite 1979. Deeper write
pipeline (channel+barriers) left as later option if writes bursty.

Tier 3 remaining (general robustness, not ivm-bench-blocking) =
continuous RunAsync + poll cadence (DrainAsync/DrainPipelinedAsync are one-shot drains;
read-ahead + backpressure now DONE), write-behind, error/retry/health
(Feldera max_retries/UNHEALTHY), changelog exactly-once via Delta txn app-id (truncate already
idempotent), Parquet Arrow-schema-without-reading-a-batch, G3 typed-contributor refactor.
NOTE: engineered-wood has its OWN ITableFileSystem (EngineeredWood.IO) DISTINCT from
DbspNet.Core.IO.ITableFileSystem — structurally similar, different types; Delta uses EW's
fs, Snapshot uses DbspNet's; they coexist (maybe an adapter later). Then phase 4 mini
TPC-DI e2e → real ivm-bench harness.

Related: [[ivm-bench-arc]], [[dbspnet-overview]], the stored-output work
(docs/design-stored-output.md).
