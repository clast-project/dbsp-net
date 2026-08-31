# Feldera findings — Axis §2: row representation & per-row execution cost

Checkout: `d:\src\feldera`, git `78afc9077` (2026-08-29). All paths below are relative to that root.
**Nothing was built or run.** Every claim is source-read; inferences are marked **[INFERRED]**, prose-doc
claims are marked **[DOC]**.

---

## Headline

Feldera's per-tuple cost is low for one structural reason that cuts across every question below:

> **There is no hash map and no dictionary anywhere on Feldera's row path.** Every batch is a
> *sorted, immutable, trie-layered* structure. Joins are merge/gallop joins. Aggregation walks
> contiguous runs of equal keys. Hashing exists *only* to pick a worker at an exchange boundary,
> and it hashes **the key, once per distinct key**, never the whole row and never per tuple.

DbspNet's two measured cost centres — ~50–60% fresh-dictionary allocation and ~40–48% whole-row
hashing — are both *consequences of choosing a hash-indexed Z-set*. Feldera does not pay either
because it never builds one.

The second structural fact: Feldera deliberately went the *opposite* way from DbspNet's "typed path".
They **erased** their runtime to `dyn` trait objects on purpose, and then bought the performance back
by making every hot operation **range/slice-shaped**, so one virtual call amortises over a whole run
of rows.

---

## Q1. What is a row, concretely, at runtime?

### Named struct or tuple? — **Positional generic tuple structs, `TupN<T1..TN>`**

The SQL compiler's type emitter maps the relational ROW type to `Tup` + arity:

- `sql-to-dbsp-compiler/SQL-compiler/src/main/java/org/dbsp/sqlCompiler/ir/type/DBSPTypeCode.java:46`
  — `TUPLE("ROW", "", "Tup")`.
- `.../compiler/backend/rust/ToRustInnerVisitor.java:2716-2735` — `preorder(DBSPTypeTuple)` emits
  `Tup<N><T1, T2, …>`.

`TupN` is a plain Rust tuple struct with native field layout:

- `crates/feldera-macros/src/tuples.rs:67` and `:77` —
  `pub struct #name<#(#generics),*>( #(pub #generics),* );`
- `crates/dbsp/src/utils/tuple.rs:79-88` — `Tup1` … `Tup10` are pre-declared in the `dbsp` crate.
- `.../backend/rust/RustWriter.java:218-245` — the SQL compiler emits `declare_tuple! { TupN<…> }`
  for any arity > 10 that the program actually uses.

Named structs *are* also generated, but only as the **serialization boundary** for tables and views
(`ToRustInnerVisitor.java:2705-2712` emits a reference to `DBSPTypeStruct` as a bare name). See the
verbatim sample in §Q1-sample below: `struct PERSON_0 { field: String, field_0: Option<i32>, … }`
exists solely to carry column *names* for `deserialize_table_record!` / `serialize_table_record!`,
with `From` impls in both directions to `Tup3<…>`. **Inside the circuit only the `TupN` exists.**

### NULLs — **`Option<T>` per field, no bitmap in memory**

`ToRustInnerVisitor.java:2638-2650`:

```java
void optionPrefix(DBSPType type) { if (type.mayBeNull && !this.compact) this.builder.append("Option<"); }
void optionSuffix(DBSPType type) { if (type.mayBeNull) { … this.builder.append(">"); } }
```

So a nullable INT column is literally `Option<i32>` (5 bytes, padded to 8). There *is* a null bitmap
(`TupleBitmap<N>`, `crates/dbsp/src/utils/tuple.rs:14-66`, plus `TupleFormat::{Sparse,Dense}` at
`:67-71`) but reading `crates/feldera-macros/src/tuples.rs:1-9,38-43` it is used **only in the rkyv
archived/on-storage layout, and only for tuples with more than 8 fields** ("For small tuples we keep
the legacy layout… `let use_legacy = num_elements <= 8;`"). In RAM, nullability is `Option<T>`.

### SQL scalar lowering — all unboxed, all fixed-width or refcounted

`DBSPTypeCode.java:5-60` is the complete table. The ones you asked about:

| SQL | Rust | Representation | Evidence |
|---|---|---|---|
| `VARCHAR` | `SqlString` | newtype over **`ArcStr`** — refcounted immutable string; clone = atomic increment | `crates/sqllib/src/string.rs:37,45` (`type StringRef = ArcStr; pub struct SqlString(StringRef);`) |
| `VARCHAR INTERNED` | `InternedString` | a 128-bit id (currently reusing `Uuid`) into a shared quick_cache | `DBSPTypeCode.java:27`; `crates/sqllib/src/string_interner.rs:43-49` |
| `DECIMAL(p,s)` | `SqlDecimal<P,S>` = `Fixed<P,S>` | **const-generic** precision/scale over a single `i128` | `ToRustInnerVisitor.java:1328-1339`; `crates/sqllib/src/decimal.rs:9`; `crates/fxp/src/fixed.rs:42` |
| `TIMESTAMP` | `Timestamp` | `{ microseconds: i64 }` | `crates/sqllib/src/timestamp.rs:61-64` |
| `TIMESTAMP WITH TIME ZONE` | `TimestampTz` | `{ microseconds: i64 }` | `crates/sqllib/src/timestamp.rs:373-376` |
| `DATE` | `Date` | `{ days: i32 }` | `crates/sqllib/src/timestamp.rs:2247-2250` |
| `TIME` | `Time` | `{ nanoseconds: u64 }` | `crates/sqllib/src/timestamp.rs:3348-3350` |
| `INTERVAL SECONDS` | `ShortInterval` | `{ microseconds: i64 }` | `crates/sqllib/src/interval.rs:51-53` |
| `INTERVAL MONTHS` | `LongInterval` | `{ months: i32 }` | `crates/sqllib/src/interval.rs:568-570` |
| `DOUBLE`/`REAL` | `F64`/`F32` | total-order float wrappers | `DBSPTypeCode.java:12-13` |
| `BINARY` | `ByteArray`; `UUID` → `Uuid`; `VARIANT` → `Variant`; `ARRAY`/`MAP` → `Array`/`Map` | | `DBSPTypeCode.java` |

**Consequence:** a TPC-DI-shaped 30-column SCD row is one `Tup30<…>` value whose in-memory size is
the sum of its fields — every string field is 8 bytes (`ArcStr` pointer), every decimal 16 bytes
inline, every date 4 bytes. Cloning that row into an output batch is a `memcpy` of that struct plus
one atomic increment per string. **No per-field boxing, no `object[]`, no per-row heap node.**

### <a id="q1-sample"></a>Verbatim generated Rust

From `sql-to-dbsp-compiler/using.md:230-320` (their own doc; header at `:234-235` warns it is
illustrative and drifts with the implementation, but every element of it is corroborated by the
emitters cited above):

```rust
// Automatically-generated file
fn circuit(workers: usize) -> Result<(DBSPHandle, (CollectionHandle<Tup3<String, Option<i32>, Option<bool>>, Weight>,
                                                   OutputHandle<SpineSnapshot<OrdZSet<Tup1<String>, Weight>>>, )), DbspError> {

    let (circuit, streams) = Runtime::init_circuit(workers, |circuit| {
        // CREATE TABLE `PERSON` (`NAME` VARCHAR NOT NULL, `AGE` INTEGER, `PRESENT` BOOLEAN)
        #[derive(Clone, Debug, Eq, PartialEq)]
        struct r#PERSON_0 {
            r#field: String,
            r#field_0: Option<i32>,
            r#field_1: Option<bool>,
        }
        impl From<PERSON_0> for Tup3<String, Option<i32>, Option<bool>> {
            fn from(table: r#PERSON_0) -> Self {
                Tup3::new(table.r#field,table.r#field_0,table.r#field_1,)
            }
        }
        deserialize_table_record!(PERSON_0["PERSON", 3] {
            (r#field, "NAME", false, String, None),
            (r#field_0, "AGE", false, Option<i32>, Some(None)),
            (r#field_1, "PRESENT", false, Option<bool>, Some(None))
        });
        // DBSPSourceMultisetOperator 312(32)
        let (PERSON, handlePERSON) = circuit.add_input_zset::<Tup3<String, Option<i32>, Option<bool>>, Weight>();

        // rel#36:LogicalFilter.(input=LogicalTableScan#1,condition=>($1, 18))
        // DBSPFilterOperator 332(57)
        let stream3: Stream<_, OrdZSet<Tup3<String, Option<i32>, Option<bool>>, Weight>> =
            PERSON.filter(move |t: &Tup3<String, Option<i32>, Option<bool>>, | -> bool {
                wrap_bool(gt_i32N_i32((*t).1, 18i32))
            });
        // rel#38:LogicalProject.(input=LogicalFilter#36,inputs=0)
        // DBSPMapOperator 350(79)
        let stream4: Stream<_, OrdZSet<Tup1<String>, Weight>> =
            stream3.map(move |t: &Tup3<String, Option<i32>, Option<bool>>, | -> Tup1<String> {
                Tup1::new((*t).0.clone())
            });
        let handleADULT = stream4.accumulate_output();
        Ok((handlePERSON, handleADULT, ))
    })?;
    Ok((circuit, streams))
}
```

Things to notice: predicates take `&TupN` (no copy); the map body's `.clone()` on a string field is
an `ArcStr` refcount bump; field access is `(*t).1`, a static offset, not a dictionary lookup or a
`Convert.ToInt32(row[1])`.

For the multi-crate build (the production path) each operator becomes its own function in its own
crate — `.../backend/rust/multi/SingleOperatorWriter.java:46-56`:

```
 * pub fn create_xxx(circuit: &RootCircuit, hash: Option<&'static str>,
 *                  sourceMap: &'static SourceMap, catalog: &mut Catalog,
 *                  i0: &Stream<RootCircuit, WSet<Tup1<Option<i32>>>>, … ) ->
 *                      Stream<RootCircuit, WSet<Tup1<Option<i32>>>> {
 *    let xxx = i0.sum([i1, i2]);
 *    return xxx;
 * }
```

The generated crate installs **jemalloc** as the global allocator, unconditionally in the multi-crate
writer: `.../backend/rust/BaseRustCodeGenerator.java:91-97` (`tikv_jemallocator::Jemalloc`, with heap
profiling switched on), used at `.../multi/CircuitWriter.java:232` and `.../RustFileWriter.java:115`.
(The in-repo Rust *benchmarks* use mimalloc instead — `crates/dbsp/src/mimalloc.rs`, e.g.
`crates/nexmark/benches/nexmark/main.rs:38`. So the Nexmark numbers you compare against are
mimalloc numbers; the shipped pipeline is jemalloc.)

---

## Q2. Statically monomorphised end to end, or a `dyn` boundary per row?

**There is a `dyn` boundary, and it is deliberate. Feldera erased their runtime on purpose.** This is
the single most surprising finding relative to DbspNet's typed-path arc.

`crates/dbsp/src/dynamic.rs:1-17` (module doc, verbatim):

> Operators are parameterized by the types of their input and output batches… This means that, had we
> used static typing (which is the default in Rust), the Rust compiler would generate a specialized
> implementation of each operator for each combination of input and output types that occur in the
> program. **In an earlier version of this library, this approach led to very long compilation times
> for even medium-sized programs.**
>
> The solution we adopt relies on dynamic dispatch instead. We bundle primitive operations on types
> (comparison, cloning, etc.) as object-safe traits and expose them to operators as trait objects.
> **This speeds up compilation without sacrificing much performance.**

The machinery (`crates/dbsp/src/dynamic/`): `Data`/`DataTrait` (object-safe `Ord`+`Clone`+
rkyv+`Debug`+`SizeOf`), `Erase` (concrete → trait object), `Factory` (allocate a default instance of
the concrete type behind a trait object), and dyn container traits `DynVec`, `DynPair`, `DynOpt`,
`DynSet`, `DynWeightedPairs` — *containers are modelled as trait objects rather than as containers of
boxed trait objects*, explicitly to avoid "many small allocations" (`dynamic.rs:19-25`).

`crates/dbsp/src/mono.rs:1-24` goes one step further and *forces* the few instantiations that do
exist (`DynData` key × `DynData` value × `RootCircuit`/`NestedCircuit`) to be compiled **inside the
`dbsp` crate**, so the many generated client crates don't each re-instantiate them.

### Why the erasure is cheap — three mechanisms

**(a) Downcast is a raw pointer cast; the `TypeId` check is compiled out in release.**
`crates/dbsp/src/dynamic/downcast.rs:56-66`:

```rust
unsafe fn downcast<T: AsAny>(&self) -> &T {
    unsafe {
        debug_assert_eq!(self.as_any().type_id(), TypeId::of::<T>(), …);
        &*(self as *const _ as *const T)
    }
}
```

**(b) The user's SQL closure is boxed once, at circuit-build time, and its body is fully
monomorphised.** `crates/dbsp/src/mono.rs:834-850`:

```rust
pub fn map<F, OK>(&self, map_func: F) -> Stream<RootCircuit, OrdZSet<OK>>
where OK: DBData, F: Fn(&K) -> OK + Clone + 'static,
{
    let factories = BatchReaderFactories::new::<OK, (), ZWeight>();
    self.inner()
        .dyn_map_mono(&factories, Box::new(move |k, pair| {
            let mut key = map_func(unsafe { k.downcast() });
            pair.from_vals(key.erase_mut(), ().erase_mut());
        }))
        .typed()
}
```

Per row that is: one indirect call through the boxed closure → one no-op pointer cast → a
statically-compiled, inlinable call to the generated SQL lambda. **No boxing of the row, no encode/
decode at the seam.** Compare DbspNet's finding that boxed key extraction inside operators was 72%
of the typed-path penalty: Feldera's `downcast` is exactly the operation DbspNet pays for as
`(int)(object)row[i]`, but in Rust it costs zero instructions.

**(c) Bulk operations dispatch once per *slice*, not once per row.** This is the load-bearing trick.
The dyn vector's API is range-shaped: `extend_from_range(other, from, to)`, `append_range`,
`sort_slice`, `sort_slice_unstable_by`, `advance_to(from, to, val)`, `retreat_to`, `dedup`
(`crates/dbsp/src/dynamic/vec.rs:140-245`). Each implementation does one downcast and then runs a
fully static loop:

`crates/dbsp/src/dynamic/vec.rs:407-411`:
```rust
fn extend_from_range(&mut self, other: &DynVec<Trait>, from: usize, to: usize) {
    let other = unsafe { other.downcast::<Self>() };
    LeanVec::extend_from_slice(self, &other[from..to])
}
```
`crates/dbsp/src/utils/consolidation.rs:376-385` (sort+consolidate a whole run in one vcall):
```rust
fn consolidate_paired_slices(&self, (keys, from1, to1): …, (weights, from2, to2): …) -> usize {
    let keys: &mut LeanVec<T1Type> = unsafe { keys.downcast_mut::<LeanVec<T1Type>>() };
    let weights: &mut LeanVec<T2Type> = unsafe { weights.downcast_mut::<LeanVec<T2Type>>() };
    consolidate_paired_slices(&mut keys[from1..to1], &mut weights[from2..to2])
}
```

### Where they *do* pay per-row dispatch

Honest counterweight: the cursor over a `Fallback*` batch (the default batch type, which can be
in-memory or on storage) is a boxed trait object —
`crates/dbsp/src/trace/cursor.rs:583-589`:

```rust
pub struct DelegatingCursor<'s, K, V, T, R>(pub Box<dyn ClonableCursor<'s, K, V, T, R> + Send + 's>)
```

so `step_key()`, `key()`, `val()`, `weight()` are virtual calls per row in an operator's scan loop.
They are tiny, non-allocating, pointer-arithmetic-only calls into a contiguous vector. **[INFERRED]**
the erasure therefore costs a handful of unpredictable-but-monomorphic-target indirect calls per row
and nothing else.

---

## Q3. Where do rows live? Columnar/trie, arena, rkyv, interning?

### Trie-layered, *not* row-major, *not* fully columnar

The in-memory value batch is literally the differential-dataflow `OrderedLayer` shape.
`crates/dbsp/src/trace/ord/vec/val_batch.rs:31`:

```rust
pub type VecValBatchLayer<K, V, T, R, O> = Layer<K, Layer<V, Leaf<DynDataTyped<T>, R>, O>, O>;
```

`crates/dbsp/src/trace/layers/layer.rs:59-79`:

```rust
/// A level of the trie, with keys and offsets into a lower layer.
/// In this representation, the values for `keys[i]` are found at `vals[offs[i] .. offs[i+1]]`.
pub struct Layer<K, L, O = usize> {
    pub(crate) keys: Box<DynVec<K>>,   // contiguous, sorted, unique
    pub(crate) offs: Vec<O>,           // one longer than `keys`
    pub(crate) vals: L,                // the nested layer
}
```
`crates/dbsp/src/trace/layers/leaf.rs:70-75`: `Leaf { keys: Box<DynVec<K>>, diffs: Box<DynVec<R>> }`.

So a batch is a handful of large contiguous arrays: keys / offsets / values / times / weights. That
is "columnar" at the *tuple-role* granularity (key vs value vs weight vs time), **not** at the SQL
column granularity — inside `keys`, each element is a whole `TupN` struct laid out row-major. So
Feldera did **not** build per-SQL-column arrays either. What they get from the split is: weights and
offsets are dense integer arrays scanned without touching row bytes, keys are deduplicated across all
their values, and `advance`/`seek` binary-search a densely-packed array.

`DynVec` is backed by `LeanVec`/`RawVec` — a hand-rolled type-erased contiguous vector storing
`{ptr, val_size, align, length, capacity}` and doing raw `ptr::copy_nonoverlapping` for growth and
moves (`crates/dbsp/src/dynamic/lean_vec.rs:35-49,257,282,302,1345-1349`). It is *not* an arena or a
bump allocator; it is one heap allocation per array.

### On-storage batches: rkyv, zero-copy, block-indexed

Beyond a size threshold a batch's physical home switches from `Vec*` to `File*` — the default batch
type is a two-armed enum (`crates/dbsp/src/trace/ord/fallback/wset.rs:60-68`:
`enum Inner { Vec(VecWSet<K,R>), File(FileWSet<K,R>) }`, destination chosen by
`pick_merge_destination` / `pick_insert_destination`).

The file format (`crates/dbsp/src/storage/file.rs:1-68`) is a per-column on-disk B-tree of data and
index blocks, immutable once written, rkyv-serialized, with Bloom (or exact roaring) per-batch key
filters. Crucially the read path is **zero-copy over the archived form**:
`crates/dbsp/src/storage/file/item.rs:34-53` exposes `ArchivedItem::fst()/snd()/split()` returning
`&K::Archived` straight out of the cached block, and `TupN` derives
`#[archive(compare(PartialEq, PartialOrd))]` (`crates/feldera-macros/src/tuples.rs`, and
`crates/dbsp/src/utils/tuple.rs:110-113` for `Tup0`), so **seeks and comparisons run against the
archived bytes without materialising a `TupN`**.

There *is* a slab/freelist pool, but only for storage I/O buffers: `crates/storage/src/fbuf/slab.rs:1-45`
("per-size-class freelists for power-of-two `FBuf` capacities"), thread-local + tokio-task-local.
No equivalent pool for in-memory batch arrays.

### Interning

Two levels:
- **Always-on, implicit:** `SqlString` is `ArcStr` (`crates/sqllib/src/string.rs:37,45`), so copying a
  string column between operators never copies bytes. There's also
  `SqlString::maybe_reuse(value, candidate)` (`:57-67`) which returns the existing `ArcStr` if the new
  value points at the same bytes, and `from_concat` (`:69-111`) which builds an `ArcStr` in one
  allocation instead of `String` → `ArcStr`.
- **Opt-in, explicit:** a column can be annotated `INTERNED`, and a compiler pass replaces it with a
  128-bit id backed by a sharded `quick_cache` that is itself a DBSP stream:
  `sql-to-dbsp-compiler/.../visitors/outer/intern/Intern.java:13-52` (`FindInternedInputs` →
  `RewriteInternedFields` → `DeadCode`), runtime in `crates/sqllib/src/string_interner.rs:33-49`
  (1 GiB default cache, entries from the last two steps pinned). `DBSPTypeCode.java:27` gives it its
  own type `INTERNED_STRING → InternedString`.

---

## Q4. Hashing — whole row or key columns? Cached? Which hasher?

**Feldera does not hash on the row path at all.** `crates/dbsp/src/hash.rs` is 15 lines long, and its
doc comment says what it is for:

```rust
/// Default hashing function used to shard records across workers.
pub fn default_hash<T: Hash + ?Sized>(x: &T) -> u64 {
    let mut hasher = Xxh3Default::new();
    x.hash(&mut hasher);
    hasher.finish()
}
```
(`crates/dbsp/src/hash.rs:6-11` — hasher is **xxh3**, from `xxhash_rust`.)

A repo-wide grep for `default_hash`/`default_hasher` in `crates/` returns matches in exactly these
non-test places: the exchange/shard operator, the per-shard balancer, `asof_join`, `hash_distinct`,
the `Data` blanket impl, and the adapters' input-record fingerprints. Nothing in `map`, `filter`,
`join`, `aggregate`, `distinct`, `topk`, `lag`, or any trace/batch code.

At the exchange, hashing is **on the key only, once per distinct key, not per tuple** —
`crates/dbsp/src/operator/dynamic/communication/shard.rs:451-462`:

```rust
let mut cursor = batch.consuming_cursor(None, None);
if cursor.has_mut() {
    while cursor.key_valid() {
        let b = &mut builders[cursor.key().default_hash() as usize % shards + workers.start];
        while cursor.val_valid() {
            b.push_diff_mut(cursor.weight_mut(), &mut serializer_inner);
            b.push_val_mut(cursor.val_mut(), &mut serializer_inner);
            cursor.step_val();
        }
        b.push_key_mut(cursor.key_mut(), &mut serializer_inner);
        cursor.step_key();
    }
}
```
Note also `consuming_cursor` + `*_mut` — values are **moved** into the per-worker builder, not cloned;
and because the input is sorted, each shard's output is produced already sorted, so no re-sort after
the shuffle. The shuffle is also elided when the stream is already correctly partitioned
(`shard.rs:176` `if self.stream.is_sharded()`, and `mark_sharded_if` / `try_sharded_version` in the
operators).

No hash is cached on a row or a batch — there is nowhere to cache it, because nothing needs one.
Key equality/ordering is `Ord` on the `TupN` (derived, field-by-field, short-circuiting), and lookups
are galloping searches, not probes.

**This is the cleanest single explanation of DbspNet's 40–48% whole-row-hash line item: it is not a
tax Feldera pays more cheaply, it is a tax Feldera does not pay.**

---

## Q5. Does the compiler prune columns so wide rows never reach operators that don't need them?

**Yes — very aggressively, at a level *below* relational projection pushdown, iterated to fixpoint,
and propagated all the way out into the input connectors.**

The pass is `sql-to-dbsp-compiler/.../compiler/visitors/unusedFields/` (16 files). Its doc:
`unusedFields/package-info.java:1-4` — *"Discover unused fields in closure parameters and optimize
them by rewriting the closures."*

The core transform, `unusedFields/RemoveUnusedFields.java:56-66` (class javadoc, verbatim):

> Analyze functions in operators and discover unused fields. Rewrite such operators as a composition
> of a map followed by a version of the original operator.
>
> An unused field is a field of an input parameter which does not affect the output of the function.
> An example is `f = |x| x.1`. Here `x.0` is an unused field. Such functions are decomposed into two
> functions such that `f = g(h)`, where `h` is a projection which removes the unused field
> `h = |x| x.1` … and `g` is a compressed version of the function `f`.

Key differences from DbspNet's `PruneJoinInputs`:

1. **It works on the emitted-lambda IR, not the relational algebra.** `FieldUseMap`
   (`unusedFields/FieldUseMap.java:23-52`) tracks per-field liveness of a *closure parameter*,
   recursively through nested tuples (`compressedType(depth)`, `allUsedFields(from, depth)`), so it
   prunes fields of the `(key, value)` pair inside a join's combine function, not just at plan seams.
2. **It applies to every operator kind, joins included.** `RemoveUnusedFields.java:1-30` imports
   `DBSPJoinOperator`, `DBSPJoinIndexOperator`, `DBSPJoinFilterMapOperator`, `DBSPLeftJoin*`,
   `DBSPStarJoin*`, `DBSPAsofJoinOperator`, `DBSPStreamAggregateOperator`,
   `DBSPAggregateLinearPostprocessOperator`, `DBSPFlatMapOperator`, `DBSPMapIndexOperator`.
3. **It is run to a fixpoint interleaved with dead-code, projection-fusion, common-projection
   extraction, filter trimming, window trimming and CSE.** `unusedFields/UnusedFields.java:46-77`:
   `RepeatRemove` wraps `OnePass` = `RemoveUnusedFields` → `DeadCode` → `OptimizeProjections` →
   `DeadCode` → `FindCommonProjections` → `ReplaceCommonProjections` → `TrimFilters` → `TrimWindows`
   → `CSE`, all inside a `Repeat`.
4. **It rewrites the source operator itself.** `UnusedFields.TrimInputs`
   (`UnusedFields.java:139-235`) builds a *new* `DBSPSourceMultisetOperator` with a narrower row type
   containing only the live columns, and rewrites the consuming map. Gated on
   `options.ioOptions.trimInputs` (`UnusedFields.java:243-245`) and skipped for materialized tables.
5. **It warns the user.** `UnusedFields.java:117-131` reports `"Unused column"` per unused,
   non-primary-key input column and marks it unused in the schema.
6. **The mark reaches the connectors.** `[DOC]` `docs.feldera.com/docs/sql/grammar.md:220-260`
   (`skip_unused_columns` table property) and `docs.feldera.com/docs/connectors/sources/delta.md:40`
   / `iceberg.md:170`: with that property set, the Delta and Iceberg connectors **do not read the
   unused columns out of Parquet at all**. Verified in the runtime types at
   `crates/feldera-types/src/program_schema.rs:261-266`. The doc also states the reason it is not
   default-on: a materialized table stores its ingested contents, and a later pipeline version might
   need the column.

Where it sits in the pipeline: `.../visitors/outer/CircuitOptimizer.java:99-105`

```java
this.add(new OptimizeWithGraph(compiler, g -> new OptimizeProjections(compiler, true, g, operatorsAnalyzed), 1));
this.add(new FuseExpensiveMaps(compiler));
this.add(new RemoveViewOperators(compiler, false));
this.add(new UnusedFields(compiler));
this.add(new Intern(compiler));
this.add(new CSE(compiler));
```
and `OptimizeProjections` runs three more times later (`:126`, `:158`), with `ShareIndexes` at `:127`
and `ShareInputIndexes` after lowering.

**Verdict on Q5: this is not a "narrowing win" bolted on late for Feldera — it is a first-class,
fixpoint-iterated, IR-level liveness analysis that reaches from the SQL closure body all the way to
the Parquet reader.** For the ivm-bench TPC-DI shape (wide SCD rows, most columns dead in most views)
this is plausibly worth more than everything else on this list combined. **[INFERRED]** — I did not
measure it.

Related narrowing/sharing passes in the same optimizer (`CircuitOptimizer.java:55-190`):
- `CreateStarJoins` (`:96`) / `BalancedJoins` (`:170`) — trees of binary joins are recognised and
  collapsed into one n-ary `DBSPStarJoinOperator`, so intermediate join results are never
  materialised. Reverse-engineering the keys is the hard part (`CreateStarJoins.java:33-42`).
  Runtime: `crates/dbsp/src/operator/dynamic/multijoin/star_join.rs`.
- `ChainVisitor` (`:157`, `:171`) + `ImplementChains` (`:172`) — chains of Map/MapIndex/Filter become
  a single `DBSPChainOperator` and then collapse into **one** `Map`/`FlatMap` closure
  (`ChainVisitor.java:16`; `ImplementChains.java:20-50`, which runs `shrinkMaps` and then
  `shrinkMapFilterMap` to a fixpoint before `collapse`). This is DbspNet's operator fusion, plus an
  extra step that narrows the *intermediate* tuple between the fused stages.
- `ShareIndexes` / `ShareInputIndexes` / `ShareWindowIntegrals` — arrangement reuse.
- `CSE` runs six times.
- `DecomposeExpensiveFilters` (`:81`) and `FuseExpensiveMaps` (`:101`) — cost-aware placement of
  expensive scalar work.

---

## Q6. Vectorised / batch-at-a-time inner loops?

**No SIMD** — a grep for `simd` / `target_feature` / `core::arch` across `crates/dbsp/src` and
`crates/sqllib/src` finds exactly one hit, and it is a `// we even could do it with simd` comment
(`crates/dbsp/src/operator/dynamic/aggregate/average.rs:364`).

**But heavily run-at-a-time**, which is where the win actually is:

- **Merge copies whole runs.** `crates/dbsp/src/trace/layers/layer.rs:378-397` — `merge_step` gallops
  to find how far one side stays strictly less than the other, then bulk-copies up to 1000 keys in
  one `copy_range`:
  ```rust
  Ordering::Less => {
      let step = 1 + trie1.keys.advance_to(1 + *lower1, upper1, &trie2.keys[*lower2]);
      let step = min(step, 1_000);
      self.copy_range(trie1, *lower1, *lower1 + step, map_func);
      *lower1 += step;
  }
  ```
  `copy_range` (`:1083-1110`) bottoms out in `DynVec::extend_from_range` → one downcast → a static
  loop. There are also `*_fueled` variants (`:755`, `:800`, `:988`) so merges are budgeted.
- **Joins gallop rather than scan.** `crates/dbsp/src/operator/dynamic/join.rs:1095-1099` —
  `Less => cursor1.seek_key(cursor2.key())`, `Greater => cursor2.seek_key(cursor1.key())`. Seeks use
  exponential (galloping) search with a small-linear prefix:
  `crates/dbsp/src/utils/advance_retreat.rs:29-47`, and a type-erased byte-slice version
  `advance_erased` (`:160-200`) that uses `get_unchecked` specifically because "LLVM's not smart
  enough to elide bounds checking".
- **Sort/consolidate is one call per slice** (`consolidation.rs:376-385`, above) over a radix/merge
  sorter (`crates/dbsp/src/trace/ord/merge_batcher.rs:1` — *"A general purpose `Batcher`
  implementation based on radix sort"*).
- **Aggregation walks contiguous groups off the sorted cursor** — see Q7.

**[INFERRED]** Effectively: their unit of vectorisation is "a sorted run", enforced by the data
structure, rather than "a column vector", enforced by an execution model.

---

## Q7. How do they avoid fresh-allocation-per-tick?

Three answers, in decreasing order of importance.

### (1) There is no per-tick state rebuild, because state is not a dictionary

DbspNet's cost is a *fresh dictionary per operator per tick*. Feldera's operator state is a
**spine** — a vector of immutable sorted batches. Ingesting a tick's delta is appending a batch
pointer; merging is amortised **on background threads**.
`crates/dbsp/src/trace/spine_async.rs:1-7` (module doc, verbatim):

> Implementation of a `Trace` which merges batches **in the background**. This is a "spine", a `Trace`
> that internally consists of a vector of batches. Inserting a new batch appends to the vector, and
> iterating or searching a spine iterates or searches all of the batches in the vector.

`spine_async.rs:543` — *"The merge runs on a pooled merger thread"*. So compaction work does not
appear in the step latency at all. (Contrast: DbspNet's compaction is bulk-on-threshold, synchronous.)

### (2) Per tick, an operator makes ~a handful of allocations, all exactly sized

Every operator allocates a small fixed number of scratch boxes plus one builder pre-sized from the
*actual* input cardinality. There are no adaptive size heuristics because the exact answer is known.

`crates/dbsp/src/operator/dynamic/filter_map.rs:641-671` (filter — note it never sorts, because the
input cursor is already ordered):
```rust
async fn eval(&mut self, input: &B) -> B {
    // We can use Builder because cursor yields ordered values.  This
    // is a nice property of the filter operation.
    //
    // Pre-allocating will create waste if most tuples get filtered out, since
    // the buffers allocated here can make it all the way to the output batch.
    // This is probably ok, because the batch will either get freed at the end
    // of the current clock tick or get added to the trace, where it will likely
    // get merged with other batches soon, at which point the waste is gone.
    let mut builder = B::Builder::with_capacity(&input.factories(), input.key_count(), input.len());
    let mut cursor = input.cursor();
    while cursor.key_valid() {
        if (self.filter)(cursor.key()) {
            while cursor.val_valid() {
                builder.push_diff(cursor.weight());
                builder.push_val(cursor.val());
                cursor.step_val();
            }
            builder.push_key(cursor.key());
        }
        cursor.step_key();
    }
    builder.done()
}
```

`filter_map.rs:879-905` (map): three `default_box()` scratch values per tick + `batch.reserve(i.len())`
+ `dyn_from_tuples`. Join: `join.rs:1089-1093` — one scratch output item, one buffer
`reserve(min(i1.len(), i2.len()))`.

So per tick an operator allocates O(1) objects of size O(rows), not O(rows) objects. Combined with the
fact that a row *is* `sizeof(TupN)` bytes inline in that buffer, and that jemalloc/mimalloc serve
these large size classes from thread-local caches, the "fresh allocation per tick" line item is
structurally small.

### (3) Linear aggregation needs no group dictionary at all

`crates/dbsp/src/operator/dynamic/aggregate.rs:582-615` — the classic "weigh" step. Because the input
cursor is sorted, each group is a *contiguous run*; the accumulator is three boxes reused across the
whole batch; the output builder is pre-sized to `batch.key_count()`:

```rust
let mut agg = output_factories.weight_factory().default_box();
let mut agg_delta = output_factories.weight_factory().default_box();
let mut input_weight = batch.factories().weight_factory().default_box();
let mut delta = <O::Builder>::with_capacity(&output_factories, batch.key_count(), batch.key_count());
let mut cursor = batch.cursor();
while cursor.key_valid() {
    agg.set_zero();
    while cursor.val_valid() {
        **input_weight = **cursor.weight();
        f(cursor.key(), cursor.val(), &*input_weight, agg_delta.as_mut());
        agg.add_assign(&agg_delta);
        cursor.step_val();
    }
    if !agg.is_zero() { delta.push_val_diff_mut(().erase_mut(), &mut agg); delta.push_key(cursor.key()); }
    cursor.step_key();
}
delta.done()
```

There is no `HashMap<Key, Acc>` to allocate, populate, and throw away.

### What they *don't* have

- **No arena / bump allocator** for batch data.
- **No delta-buffer pool** for in-memory batches. A grep for `recycle|pool` across
  `crates/dbsp/src/trace/` and `crates/dbsp/src/operator/` finds only the merger *thread* pool. The
  one exception is inside the merge sorter, which keeps a `stash: Vec<Box<DynWeightedPairs<D,R>>>` of
  emptied buffers and reuses them across merge rounds
  (`crates/dbsp/src/trace/ord/merge_batcher/merge_sorter.rs:17-18, 83-116, 158-170`), and the storage
  I/O slab pool (`crates/storage/src/fbuf/slab.rs`).
- **No capacity guessing.** They read `input.len()` / `input.key_count()` and use it. DbspNet's
  "adaptive delta-builder pre-sizing" is solving a problem that arises from not knowing the output
  cardinality — which for order-preserving operators on sorted input is bounded exactly.

---

## Where this contradicts or challenges a DbspNet decision

### 1. "Codegen demoted three times" — *the Feldera evidence agrees with you, loudly.*

Feldera **had** a fully monomorphised runtime and **abandoned it**, in the opposite direction from a
codegen push: `crates/dbsp/src/dynamic.rs:11-17` says static typing "led to very long compilation
times for even medium-sized programs" and that dynamic dispatch "speeds up compilation **without
sacrificing much performance**". `crates/dbsp/src/mono.rs:1-24` exists purely to *reduce* the number
of monomorphic instantiations further. And they generate Rust from SQL — the most extreme form of
codegen available — yet still concluded that per-type specialization of the *operators* wasn't worth
the build cost.

**Implication:** your three-times-demoted verdict on expression codegen looks correct, and Feldera is
independent confirmation that the prize is not in specializing the operator/expression machinery.
Their generated code buys them something different: *field access is a static offset and scalars are
unboxed*, which is a **representation** property, not a codegen property.

### 2. "Typed path made things worse (+82% alloc / +42% wall)" — *diagnosis confirmed, but the fix Feldera chose is the one you haven't tried.*

Your §23 finding was that typing hurt because the structural path shares `object[]` by reference while
typing inserts decode/encode, and that 72% of the penalty was **boxed key extraction inside
operators**. Feldera's architecture is the limit case of "erased everything" — and their erased path
is fast because (a) `downcast` is free (`downcast.rs:56-66`), and (b) every hot operation is
**slice-shaped**, so the vcall amortises over a run (`dynamic/vec.rs:140-245`,
`consolidation.rs:376-385`, `layer.rs:378-397`).

DbspNet's C# equivalent of a free downcast does not exist for value types — that is exactly the boxing
tax you measured, and `MonomorphizeWindowOrderKey` is the local fix. But the *other* half of Feldera's
answer is available to you and is representation-independent: **make the erased interface
range-shaped instead of element-shaped.** An `IRowBatch.CopyRange(dst, from, to)` /
`SortSlice(from, to)` / `AdvanceTo(from, to, key)` surface pays one interface dispatch per run rather
than one per row, regardless of whether rows are `object[]` or typed structs.

### 3. "Columnar designed twice, never built" — *Feldera didn't build SQL-column-wise columnar either. But they did build the trie split, and that is where their allocation win lives.*

`VecValBatchLayer = Layer<K, Layer<V, Leaf<T,R>, O>, O>`
(`crates/dbsp/src/trace/ord/vec/val_batch.rs:31`) separates **keys / offsets / values / times /
weights** into distinct contiguous arrays but keeps each key as a whole row-major `TupN`. So the
apportionment that told you "columnar's prize is smaller than its cost" is not obviously wrong. What
Feldera got instead is the *sorted, immutable, contiguous* property — and that is what kills both of
your cost centres at once:

- no hash → the 40–48% whole-row-hash term goes to ~0;
- no live mutable dictionary → the 50–60% fresh-dictionary term becomes "a few exactly-sized array
  allocations per operator per tick".

**This is the decision I'd flag hardest.** Your `docs/decision-trace-family.md` concluded *stop growing
spine, keep flat (dictionary-backed) as default*, on evidence that spine loses to flat 1.4–2.5× at
W=24 on Nexmark and +14% on the bulk batch. Feldera has **no flat/dictionary family at all** — the
sorted-batch structure is not merely their spill mechanism, it is what makes their per-row path
hash-free and allocation-light. If your spine loses to your dictionary on throughput, the most likely
reading is that your spine implementation hasn't yet acquired the properties that make Feldera's fast
— specifically: merge-join with galloping seek instead of probe (`join.rs:1095-1099`,
`advance_retreat.rs:29-47`); builder-only, sort-free order-preserving operators
(`filter_map.rs:645-668`); slice-granularity bulk copies during merge (`layer.rs:378-397`); background
merging off the step's critical path (`spine_async.rs:1-7,543`); and group-by as a walk over
contiguous runs rather than a dictionary (`aggregate.rs:582-615`). I cannot tell from here which of
those DbspNet's spine already has. **This is worth checking before treating the trace-family question
as settled**, because "flat beats spine" and "we are allocation-bound and hash-bound" may be the same
observation seen twice.

### 4. "Join column pruning was the single biggest lever" — *Feldera gets it structurally, and goes considerably further.*

You found projection pushdown through join to be your biggest win (q4 −50% at W=1, 2.93–4.19× at W=8).
Feldera's `UnusedFields` is the same idea generalised: liveness on *closure parameter fields*, run to
fixpoint interleaved with DCE/CSE/fusion, applied to joins/aggregates/asof-joins/star-joins/flatmaps,
extended to rewriting the **source operator's row type**, surfaced as a user warning, and pushed into
the Delta/Iceberg readers so dead columns are never decoded from Parquet
(`unusedFields/UnusedFields.java:46-77,117-131,139-245`; `RemoveUnusedFields.java:56-66`;
`grammar.md:220-260`).

For ivm-bench specifically, three Feldera passes have no DbspNet counterpart I'm aware of and all
target the wide-row shape: `TrimInputs` + `skip_unused_columns` (never read the column),
`CreateStarJoins`/`BalancedJoins` (never materialise an intermediate join result), and
`ImplementChains`' `shrinkMapFilterMap` fixpoint (narrow the tuple *between* fused stages, not just at
the ends).

### 5. Smaller deltas worth noting

- **Allocator.** Generated Feldera pipelines install jemalloc unconditionally
  (`BaseRustCodeGenerator.java:91-97`); their in-repo Nexmark benchmark uses mimalloc
  (`crates/nexmark/benches/nexmark/main.rs:38`). DbspNet is on the CLR GC. Part of the
  "allocation-bound" gap at equal allocation *counts* is allocator quality, and none of it is
  addressable from your side except by allocating less.
- **Strings.** `SqlString = ArcStr` (`crates/sqllib/src/string.rs:37,45`) plus optional explicit
  128-bit interning (`Intern.java`, `string_interner.rs`). .NET strings are already references, so
  parity on copies — but Feldera additionally has `maybe_reuse` and `from_concat`
  (`string.rs:57-111`) to avoid the allocate-then-copy pattern, and an opt-in path that shrinks a wide
  string key to 16 bytes.
- **Decimal.** `Fixed<P,S>` over a single `i128` with **const-generic** precision/scale
  (`crates/fxp/src/fixed.rs:42`) — scale is a compile-time constant, so rescaling is a constant
  multiply, not a runtime branch on a scale field.
- **Nullability at >8 fields.** The sparse/dense null-bitmap tuple layout
  (`crates/dbsp/src/utils/tuple.rs:14-71`, `crates/feldera-macros/src/tuples.rs:1-9,38-43`) is
  *storage-only*; in RAM they eat the `Option<T>` padding. Evidence that they, too, judged an
  in-memory bitmap not worth it.

### 6. What I could not determine

- Whether Feldera's `dyn` cursor calls (`cursor.rs:583-589`) are a measurable fraction of their
  per-row cost. I found no benchmark or comment quantifying it.
- Whether `trimInputs` is on by default in the production compiler invocation. I found the flag
  (`UnusedFields.java:243-245`, `options.ioOptions.trimInputs`) but did not trace its default.
- The exact behaviour of `adaptive_joins_enabled()` (`crates/dbsp/src/circuit/dbsp_handle.rs:430`,
  branched on at `mono.rs:193,270,345,421,499,578`) — there is a second join implementation family
  selected at runtime that I did not analyse.
- Any measured attribution on Feldera's side (their equivalent of your `reprbench`). I found no such
  document in the tree.
