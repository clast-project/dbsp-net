---
name: range-shaped-dispatch-repriced
description: "Range-shaped dispatch (§10 item 1) measured on our code: the prize is dispatch in many-ticks/few-rows shapes, NOT the allocation floor"
metadata:
  type: project
---

**Measured 2026-08-30 on the Mac (M4 Pro), first increment shipped as `b5f90aa`.**

`ZSet`/`IndexedZSet` `GetEnumerator` returned `IEnumerator<KVP>`, boxing Dictionary's struct
enumerator once per enumeration and making every `MoveNext`/`Current` an interface call — paid by
every operator every tick. Now returns the struct; `IEnumerable` stays as explicit interface members.

End-to-end through a fused map/filter circuit, 4 alternating A/B reps:

| shape | wall | alloc saved (share of total) |
|---|---|---|
| 100 rows x 20k ticks | **-13.5%** | 0.32% |
| 10k rows x 300 ticks | -2.3% | 0.003% |
| 8 rows x 200k ticks | inconclusive (variance both ways) | 3.0% |

**Why it matters:** this re-prices §10 item 1 of `docs/comparison-feldera-decisions.md`. The prize is
**dispatch, not allocation** — the saving is exactly 28 B per enumeration, and the dominant term
remains the fresh output dictionary per operator per tick, exactly as §17's apportionment said
([[repr-execution-apportionment]], [[per-row-execution-efficiency]]). Going further down the
range-shaped road (slice-shaped `IMultiset`: `CopyRange`/`SortSlice`/`AdvanceTo`) should be expected
to buy back dispatch only. It will not touch the Layer-A allocation floor, so do not scope it as if
it will.

**How to apply:** worth it where ticks are many and rows per tick few; ~2% on wide shapes. A Nexmark
A/B at 400k events was inconclusive — that is a wide shape, and Nexmark wall-time on this machine is
too noisy (W=14 swings of ±40%) to resolve single-digit effects. Use the deterministic
allocated-bytes probe, not Nexmark, to A/B changes of this size.

**Measurement caveat:** M4 Pro is 10P+4E, `ProcessorCount` = 14, so the benchmark's W=14 is NOT the
i9's W=14 — never compare across ([[machine-migration-to-mac]]).
