---
name: flat-ast-for-variadic-syntax
description: "For variadic SQL surface (IN-list, future GROUPING SETS, etc.) prefer a flat AST node over parser-time desugar to nested binary ops"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 51fc8f20-fa30-40ed-be39-bafeaf2b02a0
---

When adding new variadic SQL surface syntax to the DbspNet parser, **model
it as a single AST node holding `IReadOnlyList<T>`**, not as parser-time
desugar to a left-leaning chain of binary operators.

**Why:** Desugaring `expr IN (a, b, c, ..., z)` to
`((expr=a) OR (expr=b)) OR ... OR (expr=z)` builds a binary tree of depth
N. Every recursive walker (resolver, expression compiler, monotonicity
analyzer, optimizer passes) then risks a stack overflow on large lists —
.NET's practical recursion limit is ~100-200 levels. The flat node has
constant walk-depth contribution AND compiles to a single loop instead
of a short-circuiting chain.

**How to apply:**
- When you find yourself about to "desugar at parse time" something with
  N children, stop and ask: could this list ever be large in user-written
  SQL? IN-lists with thousands of values are common in generated queries.
- For variadic constructs, write the AST as
  `record Foo(IReadOnlyList<Bar> Items, ...)` and put the iteration in
  the resolver / expression compiler, not in the AST shape.
- Future cases this applies to: `GROUPING SETS (...)`, `ROLLUP (a, b, c)`,
  `CONCAT(a, b, c, ...)` if variadic, `VALUES (...), (...), ...` literal-
  table syntax, multi-row INSERT (if ever supported).

The principle isn't "always avoid binary chains" — binary AST nodes are
fine when the source syntax is genuinely binary (`a AND b`, `a OR b`,
`a + b`). It's specifically about *N-ary surface syntax* where the user
can write an unbounded list and the parser currently has no node for it.

Flagged by the user during planning of [[in-exists-implementation]] —
specifically pushed back on the original "desugar `IN (lit_list)` to OR
chain at parse time" plan.
