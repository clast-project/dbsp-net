# DbspNet — join exchange-barrier fusion (§15 gate)

A join shuffles both inputs by the join key through two independent `ExchangeIndex` rendezvous; §15 found that barrier wait is the dominant scaling cost. `CoalesceJoinExchange` fuses them into one `ExchangeIndexJoin` (publish both sides, rendezvous once). Per query: **median-of-3 step** unfused vs fused (cross-checked identical output), and the single-pass exchange **Wait%** each way (the term the fusion targets).

Stream: 1,000,000 events, batch 10k, W=24. Host: .NET 10.0.9, 24 logical cores.

| Query | Unfused step (ms) | Fused step (ms) | Step↑ | Unfused Wait% | Fused Wait% | Output |
|---|---:|---:|---:|---:|---:|---|
| q4 | 643.6 | 628.2 | 1.02× | 65% | 49% | ok |
| q9 | 491.5 | 413.9 | 1.19× | 54% | 58% | ok |
| q20 | 300.4 | 259.6 | 1.16× | 66% | 62% | ok |
| q3 | 56.4 | 47.5 | 1.19× | 96% | 92% | ok |

**Reading it.** Step↑ > 1 means fusing the two barriers into one sped the step up; the Wait% columns show the mechanism — the fused form should idle less at the exchange. Output must be `ok` (the fusion is a pure coordination change, identical math). **Note: each run is a single W.** The headline result is W-dependent and the lever is **HELD, off by default** — see design-row-representation.md §15.7: fusing helps only in the oversubscribed W=host regime (high wait) and *regresses* at W≈P-core count (the sensible operating point), because the ceiling is the straggler bound, not barrier count.

