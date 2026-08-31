# DbspNet — W=1 per-row execution cost

## W=1 per-row execution cost

Per-tuple efficiency, isolated from parallelism by running each query through a single (non-parallel) circuit. The exchange/scaling arc (§15) ruled out scaling as the cause of the residual q4/q18/q19 Feldera gaps; this measures the per-tuple cost that remains. `ns/ev` and `B/ev` are *per stream event* (the whole 1:3:46 Person:Auction:Bid stream is the denominator, so a query reading only `bid` does ~92% of the stream as real work, while a join also reading `auction` reads ~6%).

Stream: 1,000,000 events (20,000 person, 60,000 auction, 920,000 bid), batch 10,000, median of 5 runs. Allocation via `GC.GetAllocatedBytesForCurrentThread` (accurate at W=1). Host: .NET 10.0.8, 14 cores, Server GC.

| Query | Shape | ns/event | B/event | GC 0/1/2 | out rows |
|:------|:------|---------:|--------:|:---------|---------:|
| q0 | passthrough (ingest+egest boundary) | 270.2 | 653 | 1/1/1 | 9,200 |
| q1 | + 1 projection delegate (price map) | 360.5 | 769 | 1/1/1 | 9,200 |
| q2 | + filter (auction % 123 = 0) | 223.8 | 515 | 0/0/0 | 74 |
| q22 | + 3 string SPLIT_INDEX projections | 575.6 | 882 | 1/1/1 | 9,200 |
| q3 | join (auction ⋈ person, filtered) | 47.1 | 83 | 0/0/0 | 22 |
| q20 | join (bid ⋈ auction, wide output) | 609.0 | 1190 | 1/1/1 | 1,890 |
| q4 | join + nested MAX + outer AVG | 1262.5 | 2222 | 1/1/1 | 10 |
| q9 | join + partitioned TOP-1 | 582.9 | 1116 | 2/2/1 | 1,430 |
| q18 | partitioned TOP-1 dedup | 1370.4 | 2106 | 1/1/1 | 9,200 |
| q19 | partitioned TOP-10 | 1040.1 | 1668 | 1/1/1 | 8,706 |

