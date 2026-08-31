---
name: nexmark-feldera-benchmark-setup
description: How to run the DbspNet-vs-Feldera Nexmark comparison and the toolchain quirks on this machine
metadata: 
  node_type: memory
  type: project
  originSessionId: 8f814452-ad23-435d-a372-5a4c50f0d55a
---

`scripts/compare-nexmark.sh` runs the Nexmark throughput benchmark on both DbspNet (.NET) and Feldera's Rust DBSP engine and prints a merged events/s table. Both sides measure engine compute only (events pre-generated before the timer).

**Why / non-obvious facts:**
- The system `dotnet` (`/usr/local/share/dotnet`) is SDK 8 and CANNOT build this repo (targets net10.0). The usable .NET 10 SDK is at `~/.dotnet` (10.0.300). Run benchmarks with `DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH` prefixed, or they fail with NETSDK1045.
- Feldera checkout is at `../feldera`. The engine-only comparison needs `scripts/feldera-nexmark-pregen.patch` applied there (gates on `NEXMARK_PREGEN=1`); the script warns if missing. Without it Feldera measures generation+compute and the table over-credits DbspNet ~2x.
- The DbspNet benchmark runner uses its OWN built-in query set (invoked positionally as `nexmark EVENTS BATCH RUNS CORES`); the script's `--queries` flag only steers the Feldera side and the merge ordering, NOT what DbspNet runs.
- q5/q7/q8/q11/q12 are unsupported in DbspNet (need windowing table functions: HOP/TUMBLE/SESSION). DbspNet implements q0–q4, q9, q15–q20, q22.

**How to apply:** `export DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH` then `scripts/compare-nexmark.sh`. First Feldera run compiles the DBSP workspace in release (minutes); later runs skip it. Raise `--events` (e.g. 10000000) for steadier ratios — single Feldera run vs DbspNet median-of-3 at 1M events is noisy (esp. q3). Related: [[datas-gc-nexmark-throughput]].
