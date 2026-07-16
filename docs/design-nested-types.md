# Nested types: flattening now, first-class types later

Status: **ROW/struct flattening shipped.** First-class composite types (struct + array)
**deferred** — this note is the reminder of why, and what to build when the time comes.

## What ships today

`ROW(field TYPE [NULL|NOT NULL], …)` column declarations (arbitrarily nested) and
multi-level dotted field access (`cm.Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE`). Driven
by ivm-bench's `batch1_customer_mgmt` / `crm_customer_mgmt` (the only nested-data models in
that benchmark), whose every use of a struct bottoms out at a scalar leaf.

## How: flatten at DDL, no runtime struct value

A `ROW` column is expanded at `CREATE TABLE` into one scalar `SchemaColumn` per leaf,
named by its dotted path. `Customer ROW(Account ROW(CA_B_ID BIGINT, …), …)` registers as
scalar columns literally named `customer.account.ca_b_id`, `customer.account.ca_name`, …
Dotted access resolves to those leaves by ordinary name lookup. **The runtime never sees a
struct value** — a table with N leaves is byte-for-byte indistinguishable from one declared
with N scalar columns.

Change sites (all in the parser + resolver front end):

- `Parser/Ast/SqlStatements.cs` — `SqlTypeSpec.Fields` (a `RowFieldSpec` list; null for
  scalars).
- `Parser/Parser.cs` — `ParseRowTypeSpec` (recursive `ROW(...)`); the dotted-path loop in
  `ParseIdentifierExpression` (first segment → qualifier, rest joined by `.` → name);
  `ExpectFieldName` (accepts a keyword as a member name — struct fields like `Value`/`Name`
  legitimately collide with reserved words, and the position is unambiguous).
- `Parser/Lexer.cs` — `Lexer.IsKeywordKind`.
- `Plan/Resolver.cs` — `ExpandRowColumn` (recursive leaf expansion; dedup on full path;
  `TypeInference.FromSpec` called only on scalar leaves).

Everything downstream — `Schema`, `SchemaColumn`, column resolution, the `SqlType`
hierarchy, `TypeInference`, `SchemaFingerprint`, `StructuralRow`, the typed codec, the
expression compiler — is **unchanged**, because a dotted leaf name is an opaque
ordinal-compared string and each leaf is an existing scalar type.

NULL semantics: a leaf is nullable if declared nullable or under any nullable ancestor
struct. A wholly-NULL struct therefore yields a null cell at each leaf, which for a
leaf-access-only workload is indistinguishable from a per-field NULL. Nothing tracks
struct presence separately (nothing needs to).

## What this deliberately does NOT support

- **Selecting or emitting a whole struct** (`SELECT cm.Customer`, or a struct-typed output
  column). A whole-struct reference resolves to no leaf column and errors cleanly — never a
  silent wrong answer. `SELECT *` on a nested table returns the flattened leaves, not
  nested structs (a divergence from Spark, unexercised by ivm-bench).
- **`ROW(...)` value constructors**, struct comparison, struct-returning functions.
- **Arrays / `LIST` of anything.** See below — this is the load-bearing reason the
  first-class path exists.

## Why flattening does not generalize to arrays

A struct is a **product** type: a fixed, statically-known set of fields, so its leaf set is
known at DDL time and can be expanded into fixed columns. An array is a **collection**
type: variable length per row. There is no static leaf set to flatten into. The flattening
trick is specific to product types and does not stretch to arrays — neither a fixed-max
hack nor child-table normalization is a clean generalization.

So arrays force a **genuine runtime collection value in the cell**, which is exactly the
machinery flattening avoids. In Arrow terms: a `StructArray` can be flattened into leaf
child columns, but a `ListArray` cannot — an Arrow-native engine ingesting arbitrary nested
Parquet eventually needs runtime nested values regardless.

**Consequence for sequencing:** if/when arrays (or whole-struct values, or Arrow struct
round-trip) become real requirements, the flattening DDL-expansion is thrown away — but the
parser work (ROW-spec + dotted/element parsing) is reused. Roughly ~80% of today's change
survives; only `ExpandRowColumn` is replaced by keeping a composite type.

## The first-class alternative (build this when arrays or whole-struct values are needed)

Add composite `SqlType`s carried as runtime values. `SqlRowType` and `SqlArrayType` are
siblings on shared scaffolding — build them together rather than one then the other. The
scaffolding, per the scoping done for this note:

| Layer | File | What a composite type needs |
|---|---|---|
| Type system | `TypeSystem/SqlType.cs` | new composite records with a CLR representation + `Name`/`WithNullable`; struct carries an ordered field list, array an element type |
| Inference | `TypeSystem/TypeInference.cs` | `FromSpec` recursion; `CommonComparableType`/`CommonNumericType` either handle or explicitly reject composites |
| Fingerprint | `Plan/SchemaFingerprint.cs` | render fields/element in `Display` so drift detection stays sound |
| Row storage | `Core/Collections/StructuralRow.cs` | a cell may hold a composite value (nested `StructuralRow` for struct, a list for array) |
| Typed codec | `Compiler/TypedRowEmitter.cs` | extend the scalar `IsSupportedClrType` allowlist; emit IL for nested equality/hash |
| Expressions | `Compiler/TypedExpressionCompiler.cs` + a new `FieldAccessExpression` / element-access node | compile member and element access against a runtime value |
| Arrow bridge | `DbspNet.Arrow` | `StructArray` ↔ struct value and `ListArray` ↔ array value, both directions |
| Array-only | — | `UNNEST`, `array_length`, indexing, and the **incremental semantics** of array ops (an element change is a whole-row retract+insert unless normalized) |

The struct half of this is a moderate, contained project; the array half adds the genuinely
hard part (variable cardinality and its IVM implications). Flattening buys the time to defer
all of it until a workload actually needs a composite value, not just leaf access.

## Tests

- `tests/DbspNet.Tests/Sql/ResolverTests.cs` — `RowColumn_*`: flattening to dotted leaves,
  3-level access, whole-struct reference rejected, same-leaf-name-different-path (the
  benchmark's `_VALUE`-in-two-structs) not a false duplicate, true duplicate rejected,
  keyword field names, LATENESS on ROW rejected.
- `tests/DbspNet.Tests/Sql/CompilerTests.cs` — `RowColumn_EndToEnd_*`: push flat data over
  leaf columns, read nested leaves back; NULL-struct yields NULL leaves. Proves the runtime
  is untouched.
