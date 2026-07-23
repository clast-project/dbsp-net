# Design: durable identity for operators and spine batches

**Status: DESIGN; stage 1 BUILT. 2026-07-22.** Item 2 of `docs/design-layering-review.md` §8. §5 of that review
found that "identity" is currently answered three different ways, each a workaround for the same
missing concept, and that Track B would need it a fourth time. This settles what to build before
Track B rather than after.

## 0. The two halves are not the same problem

They get grouped because both are "identity", but they differ in urgency, evidence, and risk:

| | operator identity | batch identity |
|---|---|---|
| what is missing | a stable name for a stateful operator across compiles | a durable id for an immutable spine batch |
| current scheme | positional `op-{i}` by build order | positional `batch_{i}`, renumbered by compaction |
| is anything broken today? | **no** — fail-safe, see §1 | **no** — but Track B cannot be built without it |
| driver | speculative until a requirement exists | concrete: Track B |
| risk if done wrong | a checkpoint silently loads onto the wrong operator | a live batch file is deleted, or leaked forever |

**Recommendation up front: build batch identity, defer operator identity.** §1 and §2 are the
argument.

## 1. Operator identity: measure first, and the measurement says "not yet"

`Snapshot.WriteAsync` keys each operator's state as `op-{i}`, where `i` is its index in
`RootCircuit.Operators` build order. Restore is guarded by a plan fingerprint (operator types in
positional order), a schema fingerprint, and an operator count — each throwing `InvalidDataException`
on mismatch.

Nobody had written down what that implies, so `OperatorIdentityProbeTests` pins it:

- **A checkpoint survives a recompile of the same program.** Compilation is deterministic, so the
  same SQL yields the same operator order and the same keys. This is the case that matters for a
  restart, and it works.
- **A checkpoint is rejected when a program gains or loses an output view**, even though the
  operators it covers are unchanged. Indices shift, the count changes, the fingerprint mismatches,
  and the load hard-fails.

A third fact fell out of writing those tests and is worth recording, because the first version
asserted a rejection and got none: **a view that no designated output depends on is pruned and never
reaches the circuit.** Only outputs (and their transitive inputs) exist as operators, so "adding a
view" is a no-op unless the view is an output.

**Why this is a capability limit, not a defect.** The guard is fail-safe: state is never mapped onto
the wrong operator, the engine rebuilds from scratch instead. The cost is narrow — *a checkpoint does
not survive a program edit* — and nothing today asks it to. ivm-bench redeploys fresh per run. The
§10.3 CSE hazard is a *version* boundary (a pre-CSE binary's checkpoint read by a post-CSE one), and
CSE is unconditional in this tree, so there is no in-version skew.

**What building it would cost, and why the cheap version is a trap.** There is a real head start:
`RootCircuit.CurrentBuildLabel` already carries the owning view name, set by `PlanToCircuit` at two
sites, and is used today only for profiling. So `{view}/{kind}#{ordinal-within-view}` is derivable
from data the compiler already threads. But:

- The **typed compiler does not set it** — only `PlanToCircuit` does. Half the compile surface has no
  provenance, which is §3 of the layering review again.
- **CSE interning** means a shared operator belongs to several views; its id must be the canonical
  site, deterministically chosen, or the scheme is no better than positions.
- A wrong id is **worse than no id**: positions fail loudly, whereas a colliding stable id loads
  state onto the wrong operator silently. That is the §7.2 failure mode with a bigger blast radius.

So: real work, on the half of the codebase that resists change, to buy a capability nobody has asked
for, with a failure mode worse than today's. **Defer until something actually requires checkpoint
portability across a program edit** — e.g. a deployment story where views are added to a running
pipeline. If that requirement arrives, the design is `{view}/{kind}#{ordinal}` with the typed path
taught to set labels, and the guard kept as a backstop rather than replaced.

## 2. Batch identity: the one Track B actually needs

`SpineBatch` is `internal abstract` and carries **no id**. Two consequences:

- `SpineSnapshot` names batch files positionally (`prefix.batch_{i}.arrows`) from a
  level-flattened list, so compaction renumbers them. A snapshot cannot say "I reference the batch
  you wrote last time".
- Disk spill invents an id anyway — `_spillCounter`, an `Interlocked.Increment` per trace instance
  (`SpineZSetTrace`, `SpineIndexedZSetTrace`) — but it is process-local, resets on restart, and
  exists only to name a spill path.

Track B (reference-manifest snapshots) is exactly "a snapshot names batch files instead of copying
them", so it cannot be built on positional names. This is the missing concept, and it is on the
critical path.

### 2.1 What identity has to mean here

A spine batch is **immutable once sealed**. That is the property that makes identity easy and makes
reference-manifest snapshots possible at all: a batch, once written, never changes, so a file named
by its id is valid forever. Compaction does not mutate batches — it produces a *new* batch from
several inputs and drops the inputs. So:

- Every batch gets an id at construction. A merged batch is a **new** batch with a **new** id; it is
  not "the same batch, updated".
- The id must be durable across process restarts, because a restored spine holds batches written by a
  previous run. A monotone counter seeded from the restored manifest's high-water mark is sufficient
  and avoids the size and comparison costs of a UUID.
- Scope: **process-global**, revised during stage 1. The design first called for per-trace ids —
  files already live under a per-operator prefix, so per-trace uniqueness is all correctness needs,
  and it keeps seeding local. But per-trace scope requires the *trace* to be the factory for batches,
  and it is not: batches are constructed by static factories (`FromZSet`, `MergePair`, `Merge`) that
  have no trace reference. Threading one through every construction site is a large change for no
  correctness gain, since a global sequence is a strict superset of per-trace uniqueness. The only
  cost is larger numbers, and the only price is that seeding on restore becomes one global maximum
  over restored manifests rather than several local ones.

### 2.2 The part that is actually dangerous

Compaction **deletes its input spill files** (`SpineZSetTrace.Apply` → `SyncDelete`). Today that is
safe because nothing else references them. The moment a retained snapshot names a batch file, an
unconditional delete is a correctness bug that destroys a checkpoint.

So batch identity is inseparable from **lifetime**: ids are worth nothing without a rule for when a
file may be deleted. Two candidate rules:

- **Refcount** — each batch file carries a count of retained manifests naming it; compaction
  decrements, delete at zero. Precise, but the count is durable state that itself must be crash-safe,
  which is a second consistency problem.
- **Mark-and-sweep against retained manifests** — never delete on compaction; periodically list the
  retained snapshot manifests, union the batch ids they name, and delete unreferenced files. No
  durable counter, crash-safe by construction (a crash just means sweeping later), and it matches the
  existing snapshot retention model, which already prunes best-effort after the `current.txt` commit.

**Recommend mark-and-sweep.** It is the weaker mechanism and the safer one: the failure mode is a
leaked file, not a destroyed checkpoint. Refcounting's failure mode is the reverse, and this codebase
has just spent an arc on a bug whose whole character was "two mechanisms that were supposed to agree,
silently didn't".

### 2.3 Staging

1. **Ids only, no behaviour change. DONE 2026-07-22.** `SpineBatch` and `SpineIndexedBatch` carry an
   `Id`, assigned at construction from a shared monotone sequence. Snapshot still copies batches;
   full suite unchanged.

   Two details are worth recording because getting either backwards is a data-loss bug rather than a
   cosmetic one, and `SpineBatchIdentityTests` pins both:
   - **Creation vs relocation.** Compaction produces a *new* batch (new contents ⇒ new id), while
     spilling and materialising move an *existing* batch between representations (same contents ⇒
     same id). A new id on spill would orphan a referenced file; a reused id after a merge would make
     a manifest name contents it never recorded. `Merge([x])` short-circuits to `Materialise(x)` and
     so is relocation, not creation — it must not burn an id.
   - **The counter is non-generic on purpose.** A `static` field inside `SpineBatch{TKey,TWeight}`
     would be per closed constructed type, handing the same id to batches of different
     instantiations. `SpineBatchId` is a plain static class for that reason, with a test that pins it.

   Deferred to the point of need: seeding the sequence above a restored manifest's high-water mark
   (nothing persists ids yet, so there is nothing to collide with).
2. ~~**Stop deleting on compaction; sweep instead.**~~ **RETIRED 2026-07-22** — see
   `docs/decision-trace-family.md`. Stages 2 and 3 existed to serve Track B, whose premise Track A
   removes. Stage 1 stands: it is small, already built, and clarifies the model.
   ~~**Stop deleting on compaction; sweep instead.**~~ Change `SyncDelete` to a no-op and add a sweep
   over retained manifests. Behaviour-neutral for correctness, and it can be measured for leak growth
   before anything depends on it.
3. **Reference-manifest snapshots** — Track B proper, and only now cheap to build.

Step 1 is independent and safe. Step 2 is where the risk is, and it should land with a test that
retains two snapshots, compacts between them, and asserts the older snapshot still restores — which
is impossible to write today and is the real acceptance criterion for this work.

## 3. What this does not address

- Operator identity, deliberately (§1).
- The parallel path: per-worker snapshot subtrees address batches within a worker, so worker identity
  is a third axis this design does not touch.
- Whether Track B is worth building at all. `docs/design-incremental-persistence.md` §1.4 says spine
  currently costs +16% step and a worse save, and that gate is unchanged by anything here. **This
  design says what to build if Track B goes ahead; it is not an argument that it should.**
