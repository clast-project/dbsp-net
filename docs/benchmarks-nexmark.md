# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.8, 14 cores, `macOS 26.6.2`.

## Nexmark throughput

Feldera's primary published benchmark. An online-auction event stream (Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in 10,000-event micro-batches; each batch is pushed and `Step()`-ed. Throughput is **total stream events** ÷ wall-clock, the median of 3 run(s) after one warmup. This is the *cold-stream* number (every event is genuinely new); DbspNet's incremental edge shows up instead in the per-event latency benchmarks. Note the denominator is always the whole 1 : 3 : 46 stream, so a query that only reads a subset of tables (e.g. q3 reads auction + person, skipping the 92% bid majority) reports a higher events/s — it is keeping up with that much stream rate, not doing that much per-row work.

**Parallel** runs each query across W=14 data-parallel replicas (`ParallelCircuit`, hash-sharded input + exchanges at join / group-by / partitioned-TOP-K boundaries). The W>1 output is cross-checked against the W=1 replica run; a query whose plan has no correct parallel form (e.g. a global TOP-K) is marked *single-only*. Feldera-style comparison: pin W to Feldera's worker count.

Stream: 10,000,000 events (200,000 person, 600,000 auction, 9,200,000 bid). Host: .NET 10.0.8, 14 cores.

| Query | Description | W=1 (events/s) | W=14 (events/s) | Speedup | Last Δ rows | Status |
|:------|:------------|---------------:|---------------:|--------:|------------:|:-------|
| q0 | passthrough — SELECT * FROM bid | 3,383,304 | 9,692,874 | 2.86× | 9,200 | ok |
| q1 | currency conversion — map a column | 2,503,379 | 7,939,018 | 3.17× | 9,200 | ok |
| q2 | selection — WHERE auction % 123 = 0 | 4,080,333 | 9,951,114 | 2.44× | 74 | ok |
| q3 | local item suggestion — auction ⋈ person, filtered | 30,625,011 | 17,485,122 | 0.57× | 31 | ok |
| q4 | average closing price by category | 1,490,141 | 3,791,965 | 2.54× | 10 | ok |
| q9 | winning bids — top bid per auction | 1,522,247 | 3,816,368 | 2.51× | 1,413 | ok |
| q5 | hot items — sliding-window auction popularity | 399,556 | 577,297 | 1.44× | 19 | ok |
| q7 | highest bid by window — tumbling-window max price + join | 696,186 | — | — | 0 | single-only (no parallel plan) |
| q8 | monitor new users — windowed person ⋈ auction | 10,526,739 | 10,749,094 | 1.02× | 0 | ok |
| q12 | windowed bid counts — per-bidder counts per event-time window | 1,109,409 | 3,803,742 | 3.43× | 8,987 | ok |
| q15 | bidding statistics report — per-day bid/bidder/auction counts | 891,340 | 1,056,112 | 1.18× | 2 | ok |
| q16 | channel statistics report — per-channel/day bid/bidder/auction counts | 625,227 | 1,936,523 | 3.10× | 8 | ok |
| q17 | auction statistics by day | 1,453,288 | 4,253,778 | 2.93× | 2,335 | ok |
| q18 | find last bid — dedup latest bid per (bidder, auction) | 615,729 | 1,525,016 | 2.48× | 9,200 | ok |
| q19 | auction TOP-10 — ten highest bids per auction | 812,863 | 2,439,758 | 3.00× | 8,711 | ok |
| q20 | expand bid with auction — filtered bid ⋈ auction | 1,546,785 | 4,170,210 | 2.70× | 1,841 | ok |
| q22 | get URL directories — split the bid URL into path segments | 1,828,303 | 6,367,975 | 3.48× | 9,200 | ok |
| q11 | user sessions — session-window bid counts | — | — | — | — | unsupported (needs a SESSION windowing table function (Feldera omits q11 too — it has no session-window support)) |

> *Last Δ rows* is the size of the output change-set emitted by the final micro-batch (a smoke-test that the query produces output), not the full materialized view size.
>
> The `unsupported` row (q11 — session windows) requires a SESSION windowing table function that DbspNet does not yet expose (Feldera omits q11 from its own set too); it is listed explicitly so a Feldera comparison shows a declared gap, not a silent omission. The event-time windowing queries now run: q7 / q8 / q12 via `GROUP BY TUMBLE` and q5 via the `TABLE(HOP(…))` sliding-window TVF. Among the others: q9 / q18 / q19 use `ROW_NUMBER() OVER (PARTITION … ORDER …)` → a partitioned incremental TOP-K (and, in parallel, an exchange on the partition key); q20 is a filtered bid ⋈ auction join; q22 splits the bid URL with `SPLIT_INDEX`; q15 / q16 / q17 (per-day / per-channel / per-auction statistics with `COUNT(DISTINCT …)` and conditional `SUM(CASE …)` counts over a `CAST(date_time AS DATE)` group key) now parallelize too — the typed aggregate path handles expression group keys and the exchange shards on the computed key. q15 groups by day alone, so its speedup is bounded by the (small) number of distinct days, not the worker count.

