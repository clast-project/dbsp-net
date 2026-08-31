---
name: icloud-conflict-copies-break-builds
description: "The repo sits under ~/Documents (iCloud-synced); rapid file rewriting spawns 'File 2.cs' conflict copies that break the build with duplicate type definitions"
metadata:
  type: project
---

`~/Documents/GitHub/dbsp-net` is inside the iCloud-synced Documents folder. When files are rewritten
rapidly — exactly what an A/B loop does (`git checkout -- src/` then `git apply` in a tight cycle) —
iCloud creates conflict copies named `ZSet 2.cs`, `TopKOp 2.cs`, and so on, **untracked**, holding the
pre-edit content.

**Why it bites:** a stray `Foo 2.cs` is still compiled, so the build dies with
`error CS0101: the namespace already contains a definition for 'Foo'`. Hit 2026-08-31: 15 copies
appeared in one A/B session.

**The dangerous part is not the build failure, it is the silent bad measurement.** A loop that runs
`dotnet build ... | grep -c error >/dev/null` and then `dotnet run --no-build` will happily benchmark
the *previous* arm's assembly when the build fails, so both arms report the same binary and any
difference is fabricated.

**How to apply — for any A/B or measurement loop in this repo:**
1. `find . -name "* 2.cs" -not -path "./.git/*" -delete` before each build.
2. Assert `Build succeeded` in the output and skip the sample otherwise — never let `--no-build`
   proceed past a failed build.
3. Fingerprint the binary (`md5 -q src/DbspNet.Core/bin/Release/net10.0/DbspNet.Core.dll`) and print
   it with each arm; identical hashes across arms means the measurement is void.
4. Never suppress build output in a measurement loop.

Related: [[parallel-path-presizing]], [[range-shaped-dispatch-repriced]], [[machine-migration-to-mac]].
