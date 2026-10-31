# Flat-state probe — does the managed heap cost us on the data path? (MEASURED, 2026-10-31)

> **Status:** built and measured on branch `exp/flat-state-probe` (probe `c66d4d2`, raw results
> `docs/flat-state-probe-results.txt`). On Nexmark q20 the memory model is **not** where the cost
> is: hand-written code over plain managed objects runs within ±15% of the same code over flat
> pages, and **the engine runs 2.4–3.1× slower than either**. Flat memory does win clearly on
> checkpoint and restore (restore 8–14× faster, memory-mapped restore ready in 4–7 ms) and on CPU
> left for other work (35–40% less process CPU). The throughput gate set in advance (≥1.3× at W=8)
> is not met; the pause gate is met on the letter but the pauses it halves are 3–5% of wall.

## 1. The question

The question was Curt's: is the managed heap the wrong place for the data path? Two earlier arcs
had tried flat state inside the engine and lost: the paged trace store (design-paged-trace-state
§11–§16, managed and native pages) and the columnar batch-1 analysis (design-columnar-batch1
§7–§8). Both lost to the conversions at their edges — every operator around them still wanted boxed
`object[]` rows — so neither could say whether the memory model itself costs anything.

This probe holds the algorithm fixed and varies only where the data lives, outside the engine.

## 2. Method

**Workload: Nexmark q20** — `bid ⋈ auction ON b.auction = a.id WHERE a.category = 10`, 16 output
columns, 5 of them strings. Both join sides are integrated (bids unfiltered, auctions filtered); a
tick's output is `A_old ⋈ ΔB + ΔA ⋈ B_new`. Input from the Feldera-faithful generator
(`GeneratorProfile.Feldera`), 10,000-event ticks. With retractions on, each tick also retracts every
tenth bid of the tick ten back (so 10% of bids are retracted; the state ends at 90%).

**Arms** (`src/DbspNet.Benchmarks/FlatState/`):

| arm | state | strings | per-key rows |
|---|---|---|---|
| engine | q20 through SQL, typed path (`TypedPlanCompiler.TryCompileParallel`) | engine's (`Utf8String`) | engine's |
| managed | a sealed class per row, `Dictionary<long, Bucket>` | `string`, shared with the input | one inline, `Dictionary` when two or more |
| poh | fixed-size records in 2^13-record pages on the **pinned-object heap** | UTF-16 in arena pages | chain walk, own open-addressing table past 16 rows |
| native | the same code over **NativeMemory** pages | the same | the same |

- poh and native run *identical* JIT-specialised pointer code (`IPageSource` static abstract); only
  the page allocator differs. So poh vs native isolates heap residency; managed vs poh isolates
  object graph vs flat records.
- Hand-written arms shard on the join key and step their W workers with `Parallel.For`; the engine
  uses its own parallel circuit. Every arm's sink reads every output cell serially into a checksum
  (Σ w × row hash), so output is never optimised away, and the checksums are comparable.
- Only route + step + sink is timed; the generator is not. One process per run; JIT warmed on a
  300k-event throwaway instance; `DOTNET_gcServer=1 DOTNET_GCDynamicAdaptationMode=0`; 5 rounds,
  arms alternating inside each round, one run at a time on the idle M4 (14 cores).
- **Correctness:** all 160 runs agree — one output checksum per (size, retract) across all four arms,
  one state checksum across the three hand-written arms, and all 160 restores verified.

**Three versions of the flat store were needed.** The first two lost to managed objects for reasons
of structure, not memory model (10M events, retractions on, one run each, ns/event W=1 / W=8):

| flat layout | native | managed |
|---|--:|--:|
| one page per column (a row insert touches ~14 pages) | 480 / 227 | 281 / 177 |
| row records, one global whole-row index (half the bids go to a few hot auctions; each was a random miss into a table over every row) | 350 / 187 | 270 / 180 |
| **row records, per-key chains then per-key tables** (mirrors the managed arm's inline-one / Dictionary-when-many) | 319 / 170 | 290 / 195 |

The column layout is committed (`38082c1`); the global-index version was never committed and lives
only in this table. The lesson travels: for per-row hash-join state, a flat layout has
to keep a key's rows and index local, or it loses to the GC heap's bump allocation and per-key
Dictionaries, whatever the memory model.

## 3. Results (medians of 5; full ranges in the raw file)

### 3.1 Throughput, ns per stream event

| events | retract | W | engine | managed | poh | native | managed ÷ poh | managed ÷ native |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 1M | 0 | 1 | 776 | 245 | 267 | 256 | 0.92 | 0.96 |
| 1M | 0 | 8 | 561 | 232 | 270 | 196 | 0.86 | 1.18 |
| 1M | 1 | 1 | 923 | 337 | 320 | 357 | 1.05 | 0.94 |
| 1M | 1 | 8 | 682 | 280 | 287 | 212 | 0.98 | 1.32 |
| 10M | 0 | 1 | 732 | 238 | 281 | 259 | 0.85 | 0.92 |
| 10M | 0 | 8 | 468 | 160 | 158 | 150 | 1.01 | 1.07 |
| 10M | 1 | 1 | 867 | 290 | 337 | 319 | 0.86 | 0.91 |
| 10M | 1 | 8 | 552 | 195 | 191 | 170 | 1.02 | 1.15 |

- **At W=1, managed objects are fastest** (8–15% ahead of the flat arms). They share the input's
  strings by reference; the flat arms copy them into an arena. Input arriving as managed strings
  favours arm 1 — a connector delivering bytes would not.
- **At W=8 the flat arms catch up**, and native leads managed by 7–15% at 10M (disjoint ranges at
  10M retract: native 166–176, managed 181–201). The 1M W=8 ratios (1.18, 1.32) rest on managed runs
  ranging 144–242 and 209–316 — not robust.
- **No arm decays with state**: per-segment times are flat 1M→10M for all four.
- **native beats poh by 4–12%** throughout, beyond noise at 10M W=8 (166–176 vs 184–199).

### 3.2 GC and memory (10M events)

| retract | W | arm | GC pause in timed region | gc0/1/2 | alloc B/ev | live state | process CPU |
|--:|--:|---|--:|--:|--:|--:|--:|
| 0 | 1 | engine | 584 ms | 5/2/1 | 1312 | 3.3 GB | 23.9 s |
| 0 | 1 | managed | 2 ms | 0/0/0 | 154 | 4.0 GB | 16.9 s |
| 0 | 1 | native | 0 | 0/0/0 | 0 | 3.2 GB | 10.7 s |
| 1 | 8 | engine | 814 ms | 10/7/4 | 1842 | 3.1 GB | 34.0 s |
| 1 | 8 | managed | 96 ms | 2/2/2 | 161 | 3.7 GB | 22.0 s |
| 1 | 8 | poh | 17 ms | 2/2/2 | 331 | 3.1 GB | 14.6 s |
| 1 | 8 | native | 0 | 0/0/0 | 1 | 3.1 GB | 14.0 s |

- **Retained managed state by itself barely pauses.** 3.7–4.0 GB of live objects cost the managed
  arm 2–96 ms of pause over a 1.6–2.9 s run. It allocates only what it keeps (154 B/event), so under
  Server GC's large gen-0 budget it hardly collects.
- **The engine's pauses come from churn, not state:** it holds *less* live state than the managed
  arm (3.1–3.3 vs 3.7–4.0 GB) but allocates 8–11× more per event (1.3–1.8 KB), and pays 0.6–0.8 s of
  pause. This agrees with design-row-representation §27: the cost unit is young objects and their
  cards, not surviving bytes.
- **The GC's hidden cost is CPU.** Same generator in every arm, yet the managed arm's process uses
  6–8 s more CPU than native (+50–60%): background GC marking the live object graph on otherwise idle
  cores. On an idle 14-core box that never reaches throughput; on a loaded one it would.
- Flat state is 15–20% smaller than managed objects even with UTF-16 strings; UTF-8 would roughly
  halve the string bytes.

### 3.3 Checkpoint and restore (10M events)

| retract | W | arm | write | file | restore (copy in) | full scan after | mapped: ready / first probe / full scan |
|--:|--:|---|--:|--:|--:|--:|--:|
| 0 | 1 | managed | 1416 ms | 1.5 GB | 2449 ms | 714 ms | — |
| 0 | 1 | poh | 561 ms | 3.2 GB | 327 ms | 684 ms | — |
| 0 | 1 | native | 562 ms | 3.2 GB | 220 ms | 681 ms | 7 ms / 15 µs / 773 ms |
| 1 | 8 | managed | 813 ms | 1.3 GB | 842 ms | 89 ms | — |
| 1 | 8 | poh | 556 ms | 3.1 GB | 101 ms | 88 ms | — |
| 1 | 8 | native | 556 ms | 3.1 GB | 59 ms | 87 ms | 4 ms / 76 µs / 107 ms |

- **Restore is 8–14× faster flat** (no rebuild: pages are read back as they were), and a mapped
  restore is ready in 4–7 ms with a full scan only ~20% slower than over copied pages (warm page
  cache — a cold-cache mapped restore was not measured).
- **Write is 1.5–2.5× faster flat**, but the file is ~2.3× bigger: UTF-16, page slack, free slots and
  the per-key tables are all written as they lie. A real format would trim that.
- Managed-arm checkpoint here is a plain `BinaryWriter`; the engine's Arrow snapshot was not
  measured in this probe (design-durable-state §1b has it for ivm-bench: 4.6 GB, 11.3 GiB allocated).

### 3.4 String input as UTF-8 bytes (`FLATSTATE_INPUT=utf8`, 10M events, retractions on)

§3.1 fed every arm the generator's managed `string`s, which the managed arm keeps by reference for
free. A connector delivers bytes instead, so this run packs each tick's strings into one UTF-8
buffer before the timer (Arrow-style; every cell a `Utf8String` slice). The managed arm decodes each
cell to a `string`; the flat arms copy bytes into a UTF-8 arena (the store is generic over
`char`/`byte`); the engine keeps the slices (its boundary encoder passes `Utf8String` through). Both
modes were re-measured together on `1ccac66`, 5 runs each, all checksums and restores agreeing
(raw: `docs/flat-state-probe-results-utf8.txt`).

| W | arm | ns/ev, string | ns/ev, UTF-8 | alloc B/ev string → UTF-8 | live state string → UTF-8 | restore string → UTF-8 |
|--:|---|--:|--:|--:|--:|--:|
| 1 | engine | 882 | **691** | 1431 → 1007 | 3.1 → 2.9 GB | — |
| 1 | managed | 284 | 358 | 161 → 488 | 3.7 → 3.7 GB | 2167 → 2113 ms |
| 1 | poh | 345 | 324 | 331 → 216 | 3.1 → 2.0 GB | 316 → 228 ms |
| 1 | native | 327 | **313** | 0 → 0 | 3.1 → 2.0 GB | 219 → 152 ms |
| 8 | engine | 546 | **445** | 1842 → 1418 | 3.1 → 2.9 GB | — |
| 8 | managed | 197 | 201 | 161 → 488 | 3.7 → 3.7 GB | 775 → 608 ms |
| 8 | poh | 188 | 179 | 331 → 214 | 3.1 → 2.0 GB | 110 → 89 ms |
| 8 | native | 174 | **172** | 1 → 1 | 3.1 → 2.0 GB | 60 → 44 ms |

- **The W=1 order flips.** Decoding three strings per bid costs the managed arm +26% (284 → 358)
  and triples its allocation; the flat arms get slightly faster on half the bytes. Native now leads
  managed 1.14× at W=1 and 1.17× at W=8 — still under the 1.3× gate.
- **Flat state shrinks by a third** (3.1 → 2.0 GB, now 45% smaller than managed objects), and so do
  its checkpoint files (3.1 → 2.0 GB, now 1.5× managed's `BinaryWriter` file rather than 2.3×).
- **The engine gains most: −22% at W=1, −19% at W=8**, because its boundary encoder no longer
  allocates a `Utf8String` per string cell (−424 B/ev) and its GC pause falls 764 → 155 ms (W=1).
  It still runs 1.9–2.2× slower than the managed arm and 2.2–2.6× slower than native. The engine
  keeps slices of the input buffer, so a buffer stays alive while any row from it does — harmless
  here (90% of rows live), the chunk-retention risk of design-state-representation §10.H in general.
- Native's 25–49 ms of pause in this mode are collections triggered by the harness's untimed input
  conversion landing inside timed ticks; native itself allocates ~0 B/event.

## 4. Against the thresholds set before building

| decision (from the scoping) | result |
|---|---|
| poh ≥ 1.3× managed at W=8 | **No.** 1.01–1.02× at 10M; 0.86–0.98× at 1M. |
| …or GC pause halved without losing throughput | **Met on the letter** (96 → 17 ms, 61 → 27 ms at 10M W=8, throughput equal) but **vacuous**: the managed arm's pauses are 3–5% of its wall. |
| poh ≈ native? | **Native ahead by 4–12%**, and only native can map a checkpoint. |
| engine > 2× slower than managed? | **Yes: 2.4–3.1×.** Most of our cost on q20 is engine overhead. |

## 5. What this says

1. **On this workload the managed heap is not what makes us slow.** Typed managed objects, holding
   4 GB of state, run as fast as flat pages; their pauses are negligible because they allocate only
   what they keep.
2. **The engine is the gap — 2.4–3.1× against the same algorithm in hand-written managed C#.** It
   allocates 8–11× more per event and pays for it in pause and CPU. That is the per-row churn the
   earlier arcs named (fresh Z-sets per operator per tick, `object[]`/boxing at boundaries, whole-row
   hashing), now measured against a managed floor, not against Feldera. Closing it needs no unmanaged
   memory: the managed arm shows what typed rows + per-key buckets + reused output buffers achieve on
   the GC heap.
3. **Flat memory's real wins are durability and CPU headroom**, not step throughput: restore 8–14×
   faster (milliseconds when mapped), write 1.5–2.5× faster, 35–40% less process CPU, no GC exposure
   at all. Those matter for the durable-state arc (restore latency, checkpoint cost) and on loaded or
   oversubscribed machines.
4. **Native over POH** if flat state is ever built: consistently faster, and memory mapping needs it.
   Its price is a real release protocol (this probe frees deterministically at dispose; the engine's
   batch/trace lifetimes would need one).

## 6. Caveats

- One query. q20 is a join with wide string rows and append-mostly state; aggregates, TOP-K and
  windows were not tested (q4 was the planned second workload).
- §3.1–§3.3 feed managed strings, which favours the managed arm (zero-copy) and makes the flat arms
  store UTF-16; §3.4 re-measures with UTF-8 input, where the managed arm pays to decode.
- The machine was idle: background GC got 6 spare cores for free at W=8.
- The managed arm is hand-specialised to q20 (typed fields, one row class per table); the engine
  cannot be that specific without codegen. Its number is a floor for a managed engine, not a forecast.
- Mapped restore was measured with a warm page cache.

## 7. Reproduce

```
scripts/measure-flatstate.sh                       # 5 rounds, ~30 min, no Docker
dotnet run -c Release --project src/DbspNet.Benchmarks -- flatstate native 8 10000000 1 10000 5 /tmp/ck
```
