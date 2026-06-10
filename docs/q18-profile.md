# DbspNet — q18 profile (TOP-K op vs wide-row movement)

Nexmark q18 — *dedup the latest bid per `(bidder, auction)`* (TOP-1 over many tiny partitions, keeping all 7 wide bid columns). Worst non-inherent gap vs Feldera (0.44×). The whole pipeline runs as a `W`-replica parallel circuit; **split** (ingest), **step** (operators incl. the inter-worker exchange), and **gather** (egest the full output) are timed separately, and W is swept to see what scales.

Stream: 1,000,000 events, batch 10k, median of 5 run(s) after one warmup. Host: .NET 10.0.9, 24 cores.

| W | Split (ms) | Step (ms) | Gather (ms) | Total (ms) | Step ev/s | Step↑ vs W=1 | Output rows |
|--:|-----------:|----------:|------------:|-----------:|----------:|-------------:|------------:|
| 1 | 435.3 | 1589.3 | 3.3 | 2027.9 | 629,197 | 1.00× | 9,200 |
| 12 | 214.6 | 656.5 | 5.7 | 876.8 | 1,523,297 | 2.42× | 9,200 |
| 24 | 266.8 | 577.9 | 3.4 | 848.1 | 1,730,549 | 2.75× | 9,200 |

**Reading it.** If **step** falls ~linearly with W (W=24 step ≈ W=1 step / 24), the cost is the TOP-K operator and parallelism works — the gap is elsewhere (split/gather, i.e. wide-row ingest/egest transcode). If step stays flat (Step↑ ≪ W), the inter-worker exchange / coordination is the bottleneck. Large **split**/**gather** relative to step point at the wide-row boundary transcode (the q0-style egest-bound profile), which no operator change fixes.

