# Starting prompt — next arc: data representation & per-tuple execution

> This is a ready-to-use kickoff prompt for the next performance arc, written so a
> fresh session starts at the real decision instead of rediscovering context. It
> deliberately frames the lever as **data representation (off the managed heap /
> unboxed / pooled) + per-tuple execution**, with *columnar* as one candidate
> among several — because our own spine result (sorted-columnar **lost** to the
> flat dictionary on fine ticks, §8.3) cautions against assuming the Feldera
> columnar answer is ours. Paste the block below into a new session.

---

**Data-representation & per-tuple execution design investigation — the §16.11 lever (design-only, no rewrite)**

The performance investigation (`docs/design-row-representation.md` §16; memory
`[[per-row-execution-efficiency]]`) established with single-core measurements
(§16.11; `D:\src\dbsp-bench-2.txt`) that DbspNet's remaining competitive gap vs
Feldera is **entirely per-row execution cost, not coordination**. Single-threaded
we lose on 11/13 Nexmark queries, often 2–5× (q4 **0.21×**, q15 0.32×, q18 0.33×,
q19 0.35×, q22 0.41×, q0 0.49×); our multi-core competitiveness comes from *better
scaling* — coordination is a **strength** (we out-scale Feldera, which
de-parallelizes on low-cardinality aggregations). The cheap, safe per-row wins are
shipped (§16.8 adaptive delta-builder pre-sizing; §16.9 lazy-boxing output
boundary).

**Reframe (read before proposing anything).** Per-row time tracks allocation, but
the cost is **not GC pauses** (§15.2: ~10 collections / 1M events). It is the
allocation-and-layout tax: (1) allocation *throughput* (2.8–5 KB/event × millions =
GB/s of bump-allocate + zero + cache eviction); (2) **boxing + per-object
indirection** (a boxed scalar or a reference-keyed `Dictionary` entry is a
pointer-chase per hash/equals); (3) **per-tick fresh `Dictionary` Z-set deltas**
(even the hand-wired Core floor allocates ~2,843 B/step with no boxing). The lever
is therefore **how rows *and Z-set state* are represented so per-tuple work does
not allocate/box/indirect** — and *columnar is only one cure.* Critically, our own
evidence argues against assuming columnar: the sorted-columnar `Spine*` substrate
**lost** to the flat `Dictionary` on our fine-grained ticks (§8.3: 1.4–2.5× slower;
the memtable only reached parity). The flat-hash *execution* model won. So a
leading candidate is **"keep the flat-hash execution that won, strip its
managed-heap costs"** — value-type keys, contiguous/pooled unboxed storage,
open-addressing over arrays instead of an object-entry `Dictionary`, reused across
ticks — which is row-oriented, unboxed, pooled, *not* columnar.

**Deliverable:** a design note (`docs/design-row-representation.md` §17). **No
rewrite.** Same discipline as the row-rep / spine / exchange arcs: design-first,
**MEASURE-FIRST**, benchmark-gated, honest about fundamental-vs-fixable. Output is a
ranked design across the representation **and** execution axes, plus the *smallest
benchmark-gated first increment that validates the thesis* — not a rewrite. Use Opus.

1. **MEASURE FIRST — decompose Feldera's single-core advantage along two axes.**
   Before proposing a representation, establish *why* Feldera is ~2–5× faster per
   tuple, and **apportion the gap between representation (allocation / boxing /
   indirection / locality) and execution (delegate dispatch / monomorphization /
   vectorization)** — do not assume it's all the heap. Extend `w1profile` and the
   `profile` hand-wired ceiling; decompose **by query class**, which are
   differently shaped: q0 boundary/ingest-bound, q22 string-scalar-bound, q4/q15–q19
   aggregate/join-internal-bound. The handwired Core path is the key probe — it
   isolates the Z-set-state allocation (no SQL boundary) from the boundary/boxing.

2. **Confront the spine lesson head-on (the central question).** We *already* built
   a sorted-columnar LSM substrate (`Spine*`, §5–§13) and it **lost** to the flat
   `Dictionary` on fine ticks (§8.3). Yet Feldera uses sorted-columnar `OrdZSet`
   and wins single-core. The design **must resolve this before recommending any
   columnar direction**: is the lever columnar *storage* (tried, lost on fine
   ticks), columnar *execution* (vectorized/monomorphized batch operators, arena
   allocation, no per-tick rebuild), or simply *getting the flat-hash model off the
   per-object heap*? Working hypothesis: the flat-hash execution model is right for
   our tick granularity; the win is making its rows and state **value-type, unboxed,
   contiguous, and pooled** — and possibly monomorphizing its inner loops — rather
   than adopting sorted-columnar storage. Treat that "unboxed pooled flat-hash" as a
   first-class candidate, not an afterthought.

3. **Brainstorm + rank the representation design space (the interesting part).**
   Survey the C# toolbox for off-heap / unboxed / pooled data and evaluate each
   against the flat-hash-wins execution model and our fine-tick reality:
   - **Unboxed value-type flat hash** — open-addressing hash table over pooled
     `T[]`/`Span<T>` with value-type keys/weights, reused across ticks (no
     per-entry object, no per-tick realloc). *Leading candidate per §2.*
   - **Columnar typed batches** (`Span<T>`-per-column) + vectorized batch-at-a-time
     operators — the Feldera/`OrdZSet` analogue, reconciled with the fine-tick
     lesson; say honestly when it would and wouldn't beat the flat hash.
   - **Arena / region allocation** for operator state (bulk-freed; kills per-tick
     allocation without changing layout) — esp. the `IndexedZSet` trace inner
     structures, where q4 (0.21×) and q15–q19 bleed worst.
   - **Off-heap / unmanaged** buffers (`NativeMemory`, `Span<byte>` over fixed
     offsets, ref structs) — literally off the GC heap; weigh the safety/complexity
     cost.
   - **Struct-of-arrays vs array-of-structs**, frozen/perfect hashing, `ArrayPool`,
     row-blob encodings — as building blocks.
   - **Execution axis:** per-schema operator **monomorphization** (IL codegen of
     whole inner loops, killing delegate dispatch + boxing). §6.3 demoted
     whole-query codegen because dispatch wasn't the binding cost *then* — that
     predates the proof that per-row is the whole gap, so **re-litigate honestly**,
     informed by §1's apportionment.
   - **Targeted non-aggregate wins** (q0 boundary encode; q22 string-scalar
     allocation) — likely far cheaper than any rewrite; separate them out so they
     aren't blocked on the big bet.

4. **Honest fundamental-vs-fixable.** Quantify how much of the gap is the
   irreducible cost of a generic managed runtime vs monomorphized Rust (GC/alloc
   throughput, bounds checks, no SIMD-by-default, delegate dispatch, object
   headers). Don't assume parity is reachable; state the realistic ceiling of a
   .NET unboxed/columnar engine.

5. **Recommend the smallest benchmark-gated first increment** that validates the
   chosen direction before any broad rewrite — e.g. convert *one* hot operator (the
   q4 aggregate or join) to the chosen representation behind a seam, gated on
   single-core q4 (`w1profile`) beating the current path, mirroring how
   `surrogatebench` and the spine prototype retired/validated options before
   rollout.

**Respect / landmines.** Study our own **q3 (2.83× single-core win)** — a real
algorithmic edge — to learn what the representation must *preserve*. Honor the
typed-compiler reflection gotcha (don't change builder signatures; use ambient
seams — `[[typed-compiler-reflection-gotcha]]`). **Surrogate keys are CLOSED**
(dominated, §14.9). **The flat dictionary beat sorted-merge on our ticks** (§5–§13)
— don't re-propose sorted storage without confronting that. **Coordination is NOT a
target** (it's a strength, §16.11). Note which not-yet-built operators (TUMBLE/HOP/
SESSION windowing TVFs, UDFs) the representation should accommodate, so the design
isn't blind to functionality the engine still needs.

**Read first:** `docs/design-row-representation.md` in full — esp. §16 (the per-row
arc) + §16.11 (the single-core proof), §5–§13 (the spine lesson), §14 (surrogate),
§15 (exchange); memories `[[per-row-execution-efficiency]]`,
`[[row-representation-design]]`, `[[surrogate-key-design]]`,
`[[parallel-pipeline-perf]]`, `[[exchange-scaling-decomposition]]`; the code
(`ZSet`/`IndexedZSet`, `ZSetBuilder`, `IncrementalAggregateOp`/`IncrementalJoinOp`,
the `Spine*` family, `TypedRowEmitter`, `StructuralRow`/`TypedStructuralRow`); and
the comparison data `D:\src\dbsp-bench.txt` (multi-core) + `D:\src\dbsp-bench-2.txt`
(single-core).
