# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.1, 10 cores, `macOS 26.5.2`.

## Nexmark throughput

Feldera's primary published benchmark. An online-auction event stream (Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in 10,000-event micro-batches; each batch is pushed and `Step()`-ed. Throughput is **total stream events** ÷ wall-clock, the median of 3 run(s) after one warmup. This is the *cold-stream* number (every event is genuinely new); DbspNet's incremental edge shows up instead in the per-event latency benchmarks. Note the denominator is always the whole 1 : 3 : 46 stream, so a query that only reads a subset of tables (e.g. q3 reads auction + person, skipping the 92% bid majority) reports a higher events/s — it is keeping up with that much stream rate, not doing that much per-row work.

**Parallel** runs each query across W=10 data-parallel replicas (`ParallelCircuit`, hash-sharded input + exchanges at join / group-by / partitioned-TOP-K boundaries). The W>1 output is cross-checked against the W=1 replica run; a query whose plan has no correct parallel form (e.g. a global TOP-K) is marked *single-only*. Feldera-style comparison: pin W to Feldera's worker count.

Stream: 1,000,000 events (20,000 person, 60,000 auction, 920,000 bid). Host: .NET 10.0.1, 10 cores.

| Query | Description | W=1 (events/s) | W=10 (events/s) | Speedup | Last Δ rows | Status |
|:------|:------------|---------------:|---------------:|--------:|------------:|:-------|
| q0 | passthrough — SELECT * FROM bid | 2,290,286 | 6,029,261 | 2.63× | 9,200 | ok |
| q1 | currency conversion — map a column | 1,850,952 | 6,728,502 | 3.64× | 9,200 | ok |
| q2 | selection — WHERE auction % 123 = 0 | 2,983,070 | 8,373,000 | 2.81× | 74 | ok |
| q3 | local item suggestion — auction ⋈ person, filtered | 13,163,680 | 11,551,372 | 0.88× | 22 | ok |
| q4 | average closing price by category | 1,056,144 | 2,417,367 | 2.29× | 10 | ok |
| q9 | winning bids — top bid per auction | 855,196 | 1,163,000 | 1.36× | 1,430 | ok |
| q5 | hot items — sliding-window auction popularity | 220,501 | 503,572 | 2.28× | 22 | ok |
| q5cte | hot items — sliding-window auction popularity (CTE-shared count) | 170,940 | 432,560 | 2.53× | 22 | ok |
| q7 | highest bid by window — tumbling-window max price + join | 514,065 | — | — | 0 | single-only (no parallel plan) |
| q8 | monitor new users — windowed person ⋈ auction | 9,528,665 | 8,812,809 | 0.92× | 5 | ok |
| q12 | windowed bid counts — per-bidder counts per event-time window | 1,250,865 | 2,697,700 | 2.16× | 7,424 | ok |
| q15 | bidding statistics report — per-day bid/bidder/auction counts | 468,852 | 481,617 | 1.03× | 2 | ok |
| q16 | channel statistics report — per-channel/day bid/bidder/auction counts | 522,684 | 846,206 | 1.62× | 8 | ok |
| q17 | auction statistics by day | 758,641 | 2,076,345 | 2.74× | 2,346 | ok |
| q18 | find last bid — dedup latest bid per (bidder, auction) | 568,775 | 1,237,580 | 2.18× | 9,200 | ok |
| q19 | auction TOP-10 — ten highest bids per auction | 653,867 | 1,545,544 | 2.36× | 8,706 | ok |
| q20 | expand bid with auction — filtered bid ⋈ auction | 1,093,587 | 2,979,840 | 2.72× | 1,890 | ok |
| q22 | get URL directories — split the bid URL into path segments | 1,452,472 | 4,506,191 | 3.10× | 9,200 | ok |
| q11 | user sessions — session-window bid counts | — | — | — | — | unsupported (needs a SESSION windowing table function (Feldera omits q11 too — it has no session-window support)) |

> *Last Δ rows* is the size of the output change-set emitted by the final micro-batch (a smoke-test that the query produces output), not the full materialized view size.
>
> The `unsupported` row (q11 — session windows) requires a SESSION windowing table function that DbspNet does not yet expose (Feldera omits q11 from its own set too); it is listed explicitly so a Feldera comparison shows a declared gap, not a silent omission. The event-time windowing queries now run: q7 / q8 / q12 via `GROUP BY TUMBLE` and q5 via the `TABLE(HOP(…))` sliding-window TVF. Among the others: q9 / q18 / q19 use `ROW_NUMBER() OVER (PARTITION … ORDER …)` → a partitioned incremental TOP-K (and, in parallel, an exchange on the partition key); q20 is a filtered bid ⋈ auction join; q22 splits the bid URL with `SPLIT_INDEX`; q15 / q16 / q17 (per-day / per-channel / per-auction statistics with `COUNT(DISTINCT …)` and conditional `SUM(CASE …)` counts over a `CAST(date_time AS DATE)` group key) now parallelize too — the typed aggregate path handles expression group keys and the exchange shards on the computed key. q15 groups by day alone, so its speedup is bounded by the (small) number of distinct days, not the worker count.

