# DbspNet — W=1 per-row execution cost

## W=1 per-row execution cost

Per-tuple efficiency, isolated from parallelism by running each query through a single (non-parallel) circuit. The exchange/scaling arc (§15) ruled out scaling as the cause of the residual q4/q18/q19 Feldera gaps; this measures the per-tuple cost that remains. `ns/ev` and `B/ev` are *per stream event* (the whole 1:3:46 Person:Auction:Bid stream is the denominator, so a query reading only `bid` does ~92% of the stream as real work, while a join also reading `auction` reads ~6%).

Stream: 1,000,000 events (20,000 person, 60,000 auction, 920,000 bid), batch 10,000, median of 4 runs. Allocation via `GC.GetAllocatedBytesForCurrentThread` (accurate at W=1). Host: .NET 10.0.9, 24 cores, Server GC.

| Query | Shape | ns/event | B/event | GC 0/1/2 | out rows |
|:------|:------|---------:|--------:|:---------|---------:|
| q0 | passthrough (ingest+egest boundary) | 664.4 | 962 | 3/2/1 | 9,200 |
| q1 | + 1 projection delegate (price map) | 836.2 | 1078 | 5/3/2 | 9,200 |
| q2 | + filter (auction % 123 = 0) | 416.3 | 582 | 3/3/1 | 74 |
| q22 | + 3 string SPLIT_INDEX projections | 1156.7 | 1139 | 5/4/2 | 9,200 |
| q3 | join (auction ⋈ person, filtered) | 97.5 | 89 | 0/0/0 | 22 |
| q20 | join (bid ⋈ auction, wide output) | 1106.6 | 1391 | 5/3/1 | 1,890 |
| q4 | join + nested MAX + outer AVG | 2197.4 | 2376 | 3/2/0 | 10 |
| q9 | join + partitioned TOP-1 | 1397.8 | 2382 | 5/2/1 | 1,430 |
| q18 | partitioned TOP-1 dedup | 2248.8 | 2417 | 4/2/1 | 9,200 |
| q19 | partitioned TOP-10 | 2945.2 | 4059 | 8/3/1 | 8,706 |

