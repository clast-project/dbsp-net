# DbspNet — W=1 per-row execution cost

## W=1 per-row execution cost

Per-tuple efficiency, isolated from parallelism by running each query through a single (non-parallel) circuit. The exchange/scaling arc (§15) ruled out scaling as the cause of the residual q4/q18/q19 Feldera gaps; this measures the per-tuple cost that remains. `ns/ev` and `B/ev` are *per stream event* (the whole 1:3:46 Person:Auction:Bid stream is the denominator, so a query reading only `bid` does ~92% of the stream as real work, while a join also reading `auction` reads ~6%).

Stream: 1,000,000 events (20,000 person, 60,000 auction, 920,000 bid), batch 10,000, median of 5 runs. Allocation via `GC.GetAllocatedBytesForCurrentThread` (accurate at W=1). Host: .NET 10.0.8, 14 cores, Server GC.

| Query | Shape | ns/event | B/event | GC 0/1/2 | out rows |
|:------|:------|---------:|--------:|:---------|---------:|
| q0 | passthrough (ingest+egest boundary) | 227.9 | 672 | 0/0/0 | 9,200 |
| q1 | + 1 projection delegate (price map) | 256.3 | 789 | 1/0/0 | 9,200 |
| q2 | + filter (auction % 123 = 0) | 171.4 | 526 | 1/0/0 | 74 |
| q22 | + 3 string SPLIT_INDEX projections | 486.8 | 899 | 1/0/0 | 9,200 |
| q3 | join (auction ⋈ person, filtered) | 37.1 | 66 | 0/0/0 | 22 |
| q20 | join (bid ⋈ auction, wide output) | 467.3 | 1080 | 0/0/0 | 1,890 |
| q4 | join + nested MAX + outer AVG | 973.8 | 1991 | 1/1/0 | 10 |
| q9 | join + partitioned TOP-1 | 333.1 | 939 | 0/0/0 | 1,430 |
| q18 | partitioned TOP-1 dedup | 417.2 | 1132 | 0/0/0 | 9,200 |
| q19 | partitioned TOP-10 | 383.7 | 1007 | 1/0/0 | 8,706 |

