# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.9, 24 cores, `Microsoft Windows 10.0.26200`.

## Nexmark throughput

Feldera's primary published benchmark. An online-auction event stream (Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in 10,000-event micro-batches; each batch is pushed and `Step()`-ed. Throughput is **total stream events** ÷ wall-clock, the median of 1 run(s) after one warmup. This is the *cold-stream* number (every event is genuinely new); DbspNet's incremental edge shows up instead in the per-event latency benchmarks. Note the denominator is always the whole 1 : 3 : 46 stream, so a query that only reads a subset of tables (e.g. q3 reads auction + person, skipping the 92% bid majority) reports a higher events/s — it is keeping up with that much stream rate, not doing that much per-row work.

**Parallel** runs each query across W=24 data-parallel replicas (`ParallelCircuit`, hash-sharded input + exchanges at join / group-by / partitioned-TOP-K boundaries). The W>1 output is cross-checked against the W=1 replica run; a query whose plan has no correct parallel form (e.g. a global TOP-K) is marked *single-only*. Feldera-style comparison: pin W to Feldera's worker count.

Stream: 500,000 events (10,000 person, 30,000 auction, 460,000 bid). Host: .NET 10.0.9, 24 cores.

| Query | Description | W=1 (events/s) | W=24 (events/s) | Speedup | Last Δ rows | Status |
|:------|:------------|---------------:|---------------:|--------:|------------:|:-------|
| q0 | passthrough — SELECT * FROM bid | 2,074,852 | 4,480,186 | 2.16× | 9,200 | ok |
| q1 | currency conversion — map a column | 1,448,993 | 3,691,102 | 2.55× | 9,200 | ok |
| q2 | selection — WHERE auction % 123 = 0 | 2,534,521 | 3,768,911 | 1.49× | 70 | ok |
| q3 | local item suggestion — auction ⋈ person, filtered | 15,301,985 | 8,907,337 | 0.58× | 35 | ok |
| q4 | average closing price by category | 808,355 | 1,886,032 | 2.33× | 10 | ok |
| q9 | winning bids — top bid per auction | 803,863 | 1,893,289 | 2.36× | 1,406 | ok |
| q5 | hot items — sliding-window auction popularity | 118,593 | 413,352 | 3.49× | 20 | ok |
| q7 | highest bid by window — tumbling-window max price + join | 472,678 | — | — | 0 | single-only (no parallel plan) |
| q8 | monitor new users — windowed person ⋈ auction | 4,839,203 | 8,081,905 | 1.67× | 5 | ok |
| q12 | windowed bid counts — per-bidder counts per event-time window | 1,067,275 | 2,988,609 | 2.80× | 5,981 | ok |
| q15 | bidding statistics report — per-day bid/bidder/auction counts | 600,513 | 603,535 | 1.01× | 2 | ok |
| q16 | channel statistics report — per-channel/day bid/bidder/auction counts | 505,171 | 1,001,163 | 1.98× | 8 | ok |
| q17 | auction statistics by day | 577,492 | 2,050,549 | 3.55× | 2,338 | ok |
| q18 | find last bid — dedup latest bid per (bidder, auction) | 545,548 | 1,748,396 | 3.20× | 9,201 | ok |
| q19 | auction TOP-10 — ten highest bids per auction | 524,244 | 1,825,145 | 3.48× | 8,620 | ok |
| q20 | expand bid with auction — filtered bid ⋈ auction | 891,697 | 2,327,598 | 2.61× | 1,702 | ok |
| q22 | get URL directories — split the bid URL into path segments | 1,272,141 | 4,122,362 | 3.24× | 9,200 | ok |
| q11 | user sessions — session-window bid counts | — | — | — | — | unsupported (needs a SESSION windowing table function (Feldera omits q11 too — it has no session-window support)) |

> *Last Δ rows* is the size of the output change-set emitted by the final micro-batch (a smoke-test that the query produces output), not the full materialized view size.
>
> The `unsupported` row (q11 — session windows) requires a SESSION windowing table function that DbspNet does not yet expose (Feldera omits q11 from its own set too); it is listed explicitly so a Feldera comparison shows a declared gap, not a silent omission. The event-time windowing queries now run: q7 / q8 / q12 via `GROUP BY TUMBLE` and q5 via the `TABLE(HOP(…))` sliding-window TVF. Among the others: q9 / q18 / q19 use `ROW_NUMBER() OVER (PARTITION … ORDER …)` → a partitioned incremental TOP-K (and, in parallel, an exchange on the partition key); q20 is a filtered bid ⋈ auction join; q22 splits the bid URL with `SPLIT_INDEX`; q15 / q16 / q17 (per-day / per-channel / per-auction statistics with `COUNT(DISTINCT …)` and conditional `SUM(CASE …)` counts over a `CAST(date_time AS DATE)` group key) now parallelize too — the typed aggregate path handles expression group keys and the exchange shards on the computed key. q15 groups by day alone, so its speedup is bounded by the (small) number of distinct days, not the worker count.

