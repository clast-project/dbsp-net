# DbspNet ↔ Feldera comparison benchmarks

Feldera-compatible workloads for cross-system performance comparison (see `research/dbsp/performance_test.md`). Both systems run the same SQL over in-process generated data; the DbspNet side is below. Run on the same host as Feldera, pinning the same core count, for an apples-to-apples read.

Host: .NET 10.0.8, 24 cores, `Microsoft Windows 10.0.26200`.

## Nexmark throughput

Feldera's primary published benchmark. An online-auction event stream (Person / Auction / Bid in the standard 1 : 3 : 46 ratio) is fed in 10,000-event micro-batches; each batch is pushed and `Step()`-ed. Throughput is **total stream events** ÷ wall-clock, the median of 3 run(s) after one warmup. This is the *cold-stream* number (every event is genuinely new); DbspNet's incremental edge shows up instead in the per-event latency benchmarks. Note the denominator is always the whole 1 : 3 : 46 stream, so a query that only reads a subset of tables (e.g. q3 reads auction + person, skipping the 92% bid majority) reports a higher events/s — it is keeping up with that much stream rate, not doing that much per-row work.

Stream: 1,000,000 events (20,000 person, 60,000 auction, 920,000 bid). Host: .NET 10.0.8, 24 cores.

| Query | Description | Throughput (events/s) | Last Δ rows | Status |
|:------|:------------|----------------------:|------------:|:-------|
| q0 | passthrough — SELECT * FROM bid | 591,106 | 9,200 | ok |
| q1 | currency conversion — map a column | 492,731 | 9,200 | ok |
| q2 | selection — WHERE auction % 123 = 0 | 989,662 | 74 | ok |
| q3 | local item suggestion — auction ⋈ person, filtered | 8,756,376 | 22 | ok |
| q4 | average closing price by category | 163,595 | 10 | ok |
| q9 | winning bids — top bid per auction | 369,375 | 1,430 | ok |

> *Last Δ rows* is the size of the output change-set emitted by the final micro-batch (a smoke-test that the query produces output), not the full materialized view size.
>
> Queries q5 / q7 / q8 (tumbling / sliding event-time windows) are omitted: they require TUMBLE / HOP windowing table functions that DbspNet does not yet expose. q9 uses `ROW_NUMBER() OVER (PARTITION … ORDER …)` which compiles to a partitioned incremental TOP-K.

## Fraud detection — rolling-window features

Card `transactions` joined to `customers`, computing per-customer rolling 1-day / 7-day / 30-day transaction **count** and **sum** as real-time ML features (Feldera's documented fraud-detection use case). Three distinct `RANGE … INTERVAL` window frames feed off one join. We load a transaction history, then measure the steady-state cost of scoring **one** new transaction (`Insert` + `Step`) — the latency that matters when fraud must be caught per swipe.

| History txns | Per-event latency | Throughput (events/s) |
|-------------:|------------------:|----------------------:|
| 10,000 | 15.90 µs | 76,394 |
| 100,000 | 17.50 µs | 55,338 |
| 500,000 | 23.70 µs | 31,389 |

The per-event latency is the headline: once the history is loaded, scoring an additional transaction touches only the affected customer's window state. It stays in the tens-of-µs range across a 50× growth in history (the slow drift reflects larger trace / window-state working sets and allocator pressure, not a full rescan) — the incremental property that makes DBSP suitable for per-transaction fraud scoring. Compare this against a from-scratch recompute of the same feature view.

