---
name: typed-compiler-reflection-gotcha
description: "Changing a CircuitBuilder stateful-operator signature breaks TypedPlanCompiler's reflected calls — update the param array too"
metadata: 
  node_type: memory
  type: project
  originSessionId: b8e59bcd-7fbb-4293-8e10-b8e75ac20bc3
---

`TypedPlanCompiler` (DbspNet.Sql) invokes the `StatefulOperators` builder extensions (`IncrementalAggregate`, `IncrementalLeftJoin`, etc.) **via reflection** — `MethodInfo.Invoke(null, new object?[]{...})` in its `InvokeIncremental*` helpers (e.g. `InvokeIncrementalAggregate` ~`TypedPlanCompiler.cs:1850`) — because the emitted typed row structs are runtime `Type`s.

**Why:** reflection does NOT apply C# optional-parameter defaults; the `object?[]` must contain exactly one entry per parameter. So adding an optional parameter to one of these builder methods compiles fine but throws `TargetParameterCountException` at runtime on every query that hits the typed path — it does NOT fail the build.

**How to apply:** whenever you change a `StatefulOperators` builder signature, grep `TypedPlanCompiler` for the matching `Invoke*` helper and update its param array (pass `null` for new opt-in args the typed path doesn't use yet). Then run the FULL test suite, not just the targeted tests — this surfaced as 92 persistence/GROUP-BY failures while the isolated new tests passed. (Hit during LATENESS Phase 1 — see [[lateness-implementation]].)
