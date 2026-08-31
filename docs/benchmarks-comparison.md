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

## Nexmark head-to-head vs Feldera — M4 Pro, 2026-08-31

Re-run on the current machine after the migration ([[machine-migration-to-mac]]). **These numbers are
not comparable to anything above or to the i9 snapshot in `design-row-representation.md` §15** — the
host changed (M4 Pro, 10P+4E, `ProcessorCount` = 14, vs the i9's 8P+8E), so "W=14" is not the same
W=14. Both engines were re-measured here, so the *ratios* are internally valid; their drift against
the old i9 ratios is not a measurement of progress.

Engine-only on both sides (`scripts/compare-nexmark.sh`, events pre-generated before the timer,
circuit build excluded, `NEXMARK_PREGEN` patch applied to the Feldera checkout). DbspNet at
`44d29d1`; Feldera at `78afc9077`.

`W=14 / Feldera` = DbspNet at 14 workers ÷ Feldera at 14 cores; **> 1.0 means DbspNet is faster**.

| Query | 1M events | 10M events | reading |
|:--|--:|--:|:--|
| q0  | 1.00× | 1.00× | parity |
| q1  | 1.22× | 0.97× | parity |
| q2  | 1.11× | 1.06× | parity |
| q3  | 2.10× | **2.79×** | win |
| q4  | 1.57× | 1.19× | win, shrinking with scale |
| q5  | 0.23× | **0.16×** | **loss** |
| q7  | n/a | n/a | no W>1 path (single circuit only) |
| q8  | 1.96× | 1.81× | win |
| q9  | 2.17× | **2.36×** | win |
| q12 | 1.05× | 0.68× | degrades with scale |
| q15 | 4.53× | 2.87× | win |
| q16 | 10.38× | 7.43× | win |
| q17 | 6.17× | 6.42× | win |
| q18 | 0.50× | **0.46×** | **loss** |
| q19 | 1.04× | 0.95× | parity |
| q20 | 1.19× | 1.52× | win |
| q22 | 1.15× | 0.79× | degrades with scale |

### How much of this to believe

**The two runs disagree by up to 37%** (q15 4.53→2.87, q12 1.05→0.68, q22 1.15→0.79), and this box
swings ±40% on Nexmark W=14 between identical runs ([[parallel-path-presizing]]). Feldera's side is
a **single run per query**; DbspNet's is a median of 3. So:

- **Only differences larger than ~1.5× are safe to read as real.** Everything in 0.8–1.3× is parity
  on this evidence, whichever side of 1.0 it lands.
- Part of the 1M→10M movement is not noise but **working-set growth**: DbspNet's absolute throughput
  falls with stream size on the stateful queries (q15 1.79M→1.06M ev/s, q16 2.96M→1.94M, q22
  8.95M→6.37M) while Feldera's barely moves (q15 394k→368k, q22 7.78M→8.07M). **Their state
  structures hold throughput as state grows; ours do not.** That is the more interesting signal in
  this table and it is consistent with the trace/representation story (§16–§17), not with noise.
- The 10M column is the better read of the two — larger state, less per-query setup amortised into
  the number.

### What survives both runs

- **Robust wins:** q3, q8, q9, q15, q16, q17 (and q20 at scale). The aggregate-heavy q15/q16/q17
  margins are the lazy merge-view's (§14.10).
- **Robust losses:** **q5 (0.16–0.23×)** and **q18 (0.46–0.50×)**. q5 is the HOP path, whose GC is
  known-unbounded (`event-time-windowing`) — it accumulates state it should be dropping, which is
  exactly the shape that would produce a loss this size. q18 is the long-standing TOP-K/wide-row gap
  (§22).
- **q9 at 2.17–2.36×** is the query §26 targeted the same day. Consistent, but **not attributed** —
  no pre-§26 run was taken on this host, so the credit is unmeasured.
- **q7 has no W>1 number** at all (typed-join residual keeps it single-circuit) — a coverage gap,
  not a slow result.

### Toolchain note

Feldera does not build on the machine's default `rustc` 1.98.0: it hits a **compiler ICE** in the
next-gen trait solver (`instantiate_and_check_impossible_predicates` on `StarJoinFuncTrait`,
`rustc_next_trait_solver/.../structural_traits.rs:1012`). Build it with the version Feldera declares
in its `Cargo.toml` (`rust-version = "1.93.1"`): `rustup toolchain install 1.93.1` once, then run the
script with `RUSTUP_TOOLCHAIN=1.93.1`. This is a rustc bug, not a Feldera or patch problem.

## Fraud detection — rolling-window features

Card `transactions` joined to `customers`, computing per-customer rolling 1-day / 7-day / 30-day transaction **count** and **sum** as real-time ML features (Feldera's documented fraud-detection use case). Three distinct `RANGE … INTERVAL` window frames feed off one join. We load a transaction history, then measure the steady-state cost of scoring **one** new transaction (`Insert` + `Step`) — the latency that matters when fraud must be caught per swipe.

| History txns | Per-event latency | Throughput (events/s) |
|-------------:|------------------:|----------------------:|
| 10,000 | 15.90 µs | 76,394 |
| 100,000 | 17.50 µs | 55,338 |
| 500,000 | 23.70 µs | 31,389 |

The per-event latency is the headline: once the history is loaded, scoring an additional transaction touches only the affected customer's window state. It stays in the tens-of-µs range across a 50× growth in history (the slow drift reflects larger trace / window-state working sets and allocator pressure, not a full rescan) — the incremental property that makes DBSP suitable for per-transaction fraud scoring. Compare this against a from-scratch recompute of the same feature view.

