---
name: datas-gc-nexmark-throughput
description: "GC DATAS heap-count adaptation throttles DbspNet's parallel Nexmark W=14 throughput; disabling it (GCDynamicAdaptationMode=0) helps both .NET 10 and .NET 11"
metadata: 
  node_type: memory
  type: project
  originSessionId: 8f814452-ad23-435d-a372-5a4c50f0d55a
---

The DbspNet.Benchmarks project runs Server GC + Concurrent GC, which enables DATAS (Dynamic Adaptation To Application Sizes) by default. DATAS dynamically shrinks the GC heap count below core count, which throttles the parallel (W=N) Nexmark throughput on this bursty per-worker-allocation workload.

**Why:** Measured 2026-06 comparing .NET 10 (`~/.dotnet`, SDK 10.0.300) vs .NET 11 preview.5 (`~/.dotnet11`, SDK 11.0.100-preview.5). .NET 11 default looked ~20–33% slower at W=14 on high-throughput queries (q0/q1/q2/q9/q19) while W=1 barely moved — a parallel-scaling signature, not JIT codegen. preview.5 retuned DATAS to shrink heaps more aggressively than net10 (net10 default loses ~2% to DATAS; net11 preview.5 loses ~27%). Setting `DOTNET_GCDynamicAdaptationMode=0` recovered all of it and net11 then matched/beat net10 — so the apparent regression was the GC default, not the runtime. Disabling DATAS also helps net10 (q2 9.07M→10.82M, q22 5.85M→6.34M events/s).

**How to apply:** For throughput benchmarking, run with `DOTNET_GCDynamicAdaptationMode=0`, or set `<GCDynamicAdaptationMode>0</GCDynamicAdaptationMode>` in DbspNet.Benchmarks.csproj (trades memory headroom for steadier W=N scaling). Re-verify at .NET 11 RTM since DATAS tuning shifts between preview and release. The low-throughput aggregation queries (q15/q16) improved on net11 independent of GC (visible at W=1).

Note: building net11 requires retargeting three files that pin a TFM — `Directory.Build.props`, `src/DbspNet.Arrow/DbspNet.Arrow.csproj`, `src/DbspNet.Persistence/DbspNet.Persistence.csproj` — and the newer SDK's analyzers trip `TreatWarningsAsErrors` (IDE0028/IDE0005), so add `-p:TreatWarningsAsErrors=false -p:EnforceCodeStyleInBuild=false` for that build. Related: [[nexmark-feldera-benchmark-setup]].
