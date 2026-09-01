# Starting prompt — next arc: lazy / file-backed restore

> Ready-to-use kickoff for the recovery-latency arc, written so a fresh session starts at the
> real decision instead of rediscovering it. The framing is deliberate: the goal is **resume
> latency for pause/resume on a stream**, and the measured apportionment says the lever is the
> *deserialize* path, not the operator-state rewrite that was the obvious candidate. Paste the
> block below into a new session.

---

**Lazy / file-backed restore — recovery-latency design investigation (design-first; measure before building)**

**The goal.** Pause a running stream, resume it later. Correctness already works: full snapshot +
restore, verified against output digests (`design-incremental-persistence.md` §7.2 found a real
silent-wrong-state bug there; §8 fixed it). What is not good is **resume latency**.

**The measured baseline** (§9/§9.1, real ivm-bench SF=3, flat family, ServerGC, M4 Pro):

- snapshot **4050.7 MiB**; restore **20–23 s**; snapshot write 11.5–17.3 s.
- Replaying small incremental batches is **free** — 62 ms for two batches (leg (d)'s step leg,
  measured directly). **Recovery is restore-dominated.**
- Restore is where a resume spends its time, and it is ~2× the cost of writing the snapshot.
  Reading is the expensive direction.

**The apportionment that picks the lever** (§10, `Snapshot.ProfileLoad`):

| kind | share of restore | rebuild beyond deserialize? |
|:--|--:|:--|
| `IncrementalJoinOp` (×37) | **37.6%** | **none** — `_trace.Integrate(loaded)` |
| `PartitionedWindowAggregateOp` (×30) | 29.7% | yes (9.6% deserialize + **19.6% rebuild**) |
| `IncrementalAggregateOp` (×11) | 13.2% | partial (cache rebuild; §8 measured it at 4.5%) |
| `PartitionedOffsetOp` (×6) | 12.9% | yes (split inferred, not measured) |
| `IntegrateOp` (×16) | 5.5% | none |
| rest (×26) | 1.0% | none |

**Most of restore is reading bytes back, not rebuilding structure.** That is the finding this arc
exists to act on.

**What Feldera does** (`comparison-feldera-decisions.md` §5, read from their source):

- `Batch::persisted()` writes an in-memory batch to disk and returns it; an already-file-backed
  batch returns `None` and is untouched (`trace/ord/fallback/wset.rs:356`).
- `Spine::save` (`spine_async.rs:2199`) persists only the **RAM residue**, writes a **path list**,
  and hands the newly-persisted batches back to the live merger.
- **Recovery is lazy**: restoring a batch reads a 512-byte trailer and a Bloom block; data pages
  come through the buffer cache. **`O(#files)`, not `O(state)`.**
- Batch files live at the storage root, shared across checkpoints; checkpoint dirs hold manifests
  and tiny per-operator files. GC is two-level (`checkpointer.rs:522`).

**The tension this arc must confront head-on.** Lazy restore wants **file-backed immutable batches**
— which is the spine/LSM shape. But: `decision-trace-family.md` says *stop growing spine*; §1.3–1.4
measured spine at **+16% step** on batch 1 and a **worse** save than flat; and §8.3 of
`design-row-representation.md` found sorted-merge **lost** to the flat dictionary on fine ticks.
Meanwhile `comparison-feldera-decisions.md` §9 row 3 says that comparison was unfair — we compared
LSM-with-in-step-compaction against a dictionary, while they merge on background threads. **Do not
assume either way. This is the first question to settle, and it is settleable by measurement.**

**Questions to answer, in this order:**

1. **What is restore actually spending 20 s on?** Apportion *within* deserialize: file I/O vs Arrow
   decode vs `ZSet`/dictionary construction vs `Integrate`. `Snapshot.ProfileLoad` gives per-operator
   totals; this needs one level deeper. Until this is known, "lazy restore" is a slogan.
2. **How much of a restore is ever touched?** Lazy pays off only if a resumed pipeline reads a
   small fraction of restored state before it would have been re-derived anyway. Measure it: after a
   resume, what share of each trace's keys does the next N ticks actually probe? If the answer is
   "most of it", lazy restore buys latency but not work, and the arc is about *deferring* cost, not
   removing it — still valuable for resume latency, but say so honestly.
3. **Is the flat family compatible with file-backing at all**, or does laziness force the spine
   substrate — and if it does, is the §1.4 spine penalty still real once compaction moves off the
   step thread (§9 row 3's experiment)?
4. **What is the ceiling?** Same discipline as every arc here: estimate before building. If lazy
   restore can only reach, say, 40% of the deserialize term, that is 20 s → ~14 s, and it should be
   compared against simply making the snapshot smaller.

**Do not re-do these — they are measured and closed:**

- **The radix-tree / indexed-Z-set re-expression of operator state** (§10, branch
  `radix-tree-state`). Ceiling **~20% of restore** for `PartitionedWindowAggregateOp`, ~28% adding
  `PartitionedOffsetOp`. Deliberately not built: it is the smaller half. Revisit only *after*
  restore stops being deserialize-bound. Note also that we **already persist as a Z-set** through
  the generic codec — there is no bespoke serializer to remove.
- **Track A ("stop checkpointing every batch", §4).** Correct for ivm-bench, where the *write* ran
  every batch. Irrelevant to pause/resume, where the write happens once by design and the read
  happens every time.
- **Arity-preserving dead-column elimination** (`design-row-representation.md` §9/§10) — ~1 GiB,
  wall-neutral.

**Measurement discipline (this codebase has been bitten by all of these):**

- **Verify, don't just time.** §7.2 was a restore that silently produced wrong state; it was caught
  only because recoveries are checked against recorded output digests. Keep that.
- **Restore wall varies ~15% between legs in one run** (§9.1: 22,640 vs 19,690 ms for the same
  snapshot). Snapshot write ranged 11.5–17.3 s for byte-identical output. Never read a single
  restore figure to better than ±2–3 s.
- **Never difference two large noisy legs to get a small one.** `(b) − (a)` has come out negative
  twice; §7.3 already retracted a 70× claim built that way. Leg (d) exists to isolate directly.
- **Allocation is host-independent; wall is not** (`design-row-representation.md` §25.1 — the i9
  `w1profile` table reproduced byte-exactly on the M4). Do not compare any wall figure to the i9
  numbers in these docs.
- `DOTNET_gcServer=1` for a Docker-faithful wall.

**Environment (all reproducible locally, no cloud):**

- Data: `docs/ivm-bench-gap-analysis.md` — datagen is **three** docker stages (datagen +
  batch-loader `init`), `SCALE_FACTOR` cannot be < 3, and `staging/` must stay batch-1-only while
  `staging-multi/` carries held-back commits for multi-batch replay.
- Probe: `IvmRecoveryProbe` with `IVM_DATA_ROOT` / `IVM_SPEC` / `IVM_STAGING_ROOT` /
  `IVM_SNAPSHOT_DIR` / `IVM_WAL_DIR` / `IVM_BATCHES=3` / `IVM_SNAPSHOT_AFTER=1` /
  `IVM_PROFILE_RESTORE=1`. It writes nothing to stdout on success — capture with
  `--logger "trx;LogFileName=x.trx"` and read the `<Message>` element.
- **`Snapshot.ProfileLoad` currently lives only on branch `radix-tree-state` (`d705abd`)**, along
  with the write-up. Merge or cherry-pick it to `main` first — the instrumentation is inert when off
  and this arc needs it.

**Deliverable for the first session: a design note with a measured ceiling, not code.** If the
ceiling does not justify the work, say so and stop — three arcs here have correctly ended that way
(§10 arity reduction, §7–§8 columnar-for-batch-1, §10 radix tree).
