---
name: decimal-cast-reflection-binding
description: Decimal casts bind Clast.DatabaseDecimal by reflection, so a dependency adding an OPTIONAL parameter breaks them at runtime — and the structural path has no test covering it
metadata:
  type: project
---

Both expression compilers build decimal casts with `typeof(X).GetMethod(...)` + `Expression.Call`:
`ScaleHelper.Rescale128` and `DecimalText.ParseDecimal128`, in **`ExpressionCompiler.cs` (structural)
and `TypedExpressionCompiler.cs` (typed)** — three sites each, six in total.

**Why:** reflection binding ignores C# optional-parameter defaults. `Expression.Call` must be handed
*every* parameter. So a dependency that appends `SomeEnum x = default` — a source-compatible,
non-breaking change for ordinary callers — breaks these call sites, either as `ArgumentException:
Incorrect number of arguments` or, when `GetMethod` is given an explicit type array, a null MethodInfo
and `ArgumentNullException (Parameter 'method')`. This is exactly what Clast.DatabaseDecimal 0.1.1 →
0.3.0 did by adding `DecimalRounding rounding = HalfEven` to both methods (2026-08-30).

**The trap:** the upgrade produced 6 test failures, *all* on the typed path. The structural path had
the identical break in 3 more places and **no test caught it** — decimal casts on the structural path
(the default execution path) are uncovered. Fixing only what the suite reports leaves production
broken. Grep both files together, always.

**How to apply:** on any Clast.DatabaseDecimal bump, patch all six sites in lockstep and pass rounding
explicitly (`DecimalRounding.HalfEven`) rather than relying on the library default — verified
empirically that 0.1.1's fixed rounding is bankers'/half-even (0.15→2, 0.25→2, 0.35→4), so HalfEven
preserves behaviour byte-for-byte. Pinning it also survives a future change of the library's default.
Consider adding structural-path decimal-cast tests before the next bump.

Related: [[typed-compiler-reflection-gotcha]] (same failure class, our own builder signatures).
