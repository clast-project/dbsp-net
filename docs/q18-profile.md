# DbspNet — partitioned TOP-K egest profile (out-of-`Step` output)

The q18/q19 partitioned-TOP-K queries (worst remaining gaps vs Feldera). The whole pipeline runs as a `W`-replica parallel circuit; **split** (ingest), **step** (operators incl. the inter-worker exchange), and **gather** (egest the full output) are timed separately, and W is swept to see what scales. *gather* here is the **out-of-`Step` output** materialisation — phase (d) in the §22 4-way decomposition; confirming it is ~0 localises the gap to the in-`Step` work.

Stream: 1,000,000 events, batch 10k, median of 3 run(s) after one warmup. Host: .NET 10.0.9, 24 cores.

## q18 — find last bid — dedup latest bid per (bidder, auction)

| W | Split (ms) | Step (ms) | Gather (ms) | Total (ms) | Step ev/s | Step↑ vs W=1 | Gather% | Output rows |
|--:|-----------:|----------:|------------:|-----------:|----------:|-------------:|--------:|------------:|
| 1 | 402.0 | 1298.2 | 4.4 | 1704.6 | 770,302 | 1.00× | 0% | 9,200 |
| 12 | 204.5 | 642.3 | 4.6 | 851.3 | 1,556,975 | 2.02× | 1% | 9,200 |
| 24 | 109.0 | 565.3 | 3.4 | 677.6 | 1,769,121 | 2.30× | 0% | 9,200 |

## q19 — auction TOP-10 — ten highest bids per auction

| W | Split (ms) | Step (ms) | Gather (ms) | Total (ms) | Step ev/s | Step↑ vs W=1 | Gather% | Output rows |
|--:|-----------:|----------:|------------:|-----------:|----------:|-------------:|--------:|------------:|
| 1 | 396.4 | 2020.7 | 3.2 | 2420.2 | 494,882 | 1.00× | 0% | 8,706 |
| 12 | 164.7 | 590.1 | 3.0 | 757.7 | 1,694,652 | 3.42× | 0% | 8,706 |
| 24 | 97.4 | 535.5 | 3.5 | 636.4 | 1,867,360 | 3.77× | 1% | 8,706 |

**Reading it.** A small **Gather%** confirms output materialisation (phase d) is **not** the cost — the parallel path decodes output lazily after `sw.Stop()`, so the gap is the in-`Step` work (TOP-K op + exchange) the step decomposition splits. If **step** falls ~`1/W` the operator parallelises and the residual is coordination.

