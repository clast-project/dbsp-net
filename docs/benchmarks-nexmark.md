# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.8, 14 cores, `macOS 27.2.0`.

## Nexmark throughput

Feldera's primary published benchmark. An online-auction event stream (Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in 10,000-event micro-batches; each batch is pushed and `Step()`-ed. Throughput is **total stream events** ÷ wall-clock, the median of 3 run(s) after one warmup. This is the *cold-stream* number (every event is genuinely new); DbspNet's incremental edge shows up instead in the per-event latency benchmarks. Note the denominator is always the whole 1 : 3 : 46 stream, so a query that only reads a subset of tables (e.g. q3 reads auction + person, skipping the 92% bid majority) reports a higher events/s — it is keeping up with that much stream rate, not doing that much per-row work.

**Parallel** runs each query across W=4 data-parallel replicas (`ParallelCircuit`, hash-sharded input + exchanges at join / group-by / partitioned-TOP-K boundaries). The W>1 output is cross-checked against the W=1 replica run; a query whose plan has no correct parallel form (e.g. a global TOP-K) is marked *single-only*. Feldera-style comparison: pin W to Feldera's worker count.

Stream: 10,000,000 events (200,000 person, 600,000 auction, 9,200,000 bid). Host: .NET 10.0.8, 14 cores.

| Query | Description | W=1 (events/s) | W=4 (events/s) | Speedup | Last Δ rows | Status |
|:------|:------------|---------------:|---------------:|--------:|------------:|:-------|
| q0 | passthrough — SELECT * FROM bid | 3,015,950 | 7,318,129 | 2.43× | 9,200 | ok |
| q1 | currency conversion — map a column | 2,299,132 | 5,386,394 | 2.34× | 9,200 | ok |
| q2 | selection — WHERE auction % 123 = 0 | 4,263,992 | 7,742,202 | 1.82× | 53 | ok |
| q3 | local item suggestion — auction ⋈ person, filtered | 25,767,624 | 16,654,398 | 0.65× | 17 | ok |
| q4 | average closing price by category | 1,938,146 | 3,453,706 | 1.78× | 10 | ok |
| q9 | winning bids — top bid per auction | 1,815,088 | 3,312,792 | 1.83× | 675 | ok |
| q5 | hot items — sliding-window auction popularity | 458,313 | 1,171,362 | 2.56× | 54 | ok |
| q7 | highest bid by window — tumbling-window max price + join | 717,641 | — | — | 0 | single-only (no parallel plan) |
| q8 | monitor new users — windowed person ⋈ auction | 18,113,376 | 14,031,939 | 0.77× | 6 | ok |
| q12 | windowed bid counts — per-bidder counts per event-time window | 2,571,071 | 4,587,162 | 1.78× | 2,135 | ok |
| q15 | bidding statistics report — per-day bid/bidder/auction counts | 2,382,782 | 3,659,250 | 1.54× | 2 | ok |
| q16 | channel statistics report — per-channel/day bid/bidder/auction counts | 2,136,424 | 3,910,977 | 1.83× | 151 | ok |
| q17 | auction statistics by day | 2,325,329 | 4,698,010 | 2.02× | 741 | ok |
| q18 | find last bid — dedup latest bid per (bidder, auction) | 1,195,579 | 2,337,414 | 1.96× | 2,996 | ok |
| q19 | auction TOP-10 — ten highest bids per auction | 1,256,205 | 2,057,301 | 1.64× | 4,368 | ok |
| q20 | expand bid with auction — filtered bid ⋈ auction | 1,885,639 | 2,811,457 | 1.49× | 1,001 | ok |
| q22 | get URL directories — split the bid URL into path segments | 1,926,587 | 4,767,593 | 2.47× | 9,185 | ok |
| q11 | user sessions — session-window bid counts | — | — | — | — | unsupported (needs a SESSION windowing table function (Feldera omits q11 too — it has no session-window support)) |

> *Last Δ rows* is the size of the output change-set emitted by the final micro-batch (a smoke-test that the query produces output), not the full materialized view size.
>
> The `unsupported` row (q11 — session windows) requires a SESSION windowing table function that DbspNet does not yet expose (Feldera omits q11 from its own set too); it is listed explicitly so a Feldera comparison shows a declared gap, not a silent omission. The event-time windowing queries now run: q7 / q8 / q12 via `GROUP BY TUMBLE` and q5 via the `TABLE(HOP(…))` sliding-window TVF. Among the others: q9 / q18 / q19 use `ROW_NUMBER() OVER (PARTITION … ORDER …)` → a partitioned incremental TOP-K (and, in parallel, an exchange on the partition key); q20 is a filtered bid ⋈ auction join; q22 splits the bid URL with `SPLIT_INDEX`; q15 / q16 / q17 (per-day / per-channel / per-auction statistics with `COUNT(DISTINCT …)` and conditional `SUM(CASE …)` counts over a `CAST(date_time AS DATE)` group key) now parallelize too — the typed aggregate path handles expression group keys and the exchange shards on the computed key. q15 groups by day alone, so its speedup is bounded by the (small) number of distinct days, not the worker count.

