# DbspNet — window-agg order-key monomorphization gate (§23.7)

Competitive A/B of keying the typed `PARTITION BY … ORDER BY` window's per-partition ordered state on the **unboxed** monotone long (`LongKeyComparer`) vs the boxed `SortKeyComparer`. Workload: the fraud rolling-window feature view (per-customer 1d/7d/30d `SUM`/`COUNT OVER … RANGE PRECEDING`, TIMESTAMP order key). Only the order comparer differs between configs; the mono output is cross-checked byte-identical to boxed.

Stream: 200,000 transactions over 2,000 customers (~100 txns/partition), 10,000-event micro-batches, median of 3 run(s) after one warmup. Host: .NET 10.0.9, 24 cores.

| W | Config | Throughput (events/s) | Speedup vs boxed | Alloc (MiB) | Alloc vs boxed |
|--:|:-------|----------------------:|-----------------:|------------:|---------------:|
| 1 | boxed | 36,600 | 1.00× | 12,461.8 | 1.000 |
| 1 | mono | 44,930 | 1.23× | 8,042.0 | 0.645 |
| 14 | boxed | 200,440 | 1.00× | 12,633.3 | 1.000 |
| 14 | mono | 242,955 | 1.21× | 8,312.3 | 0.658 |

**Reading it.** *Speedup vs boxed* > 1 means the monomorphized comparer sped the step up; *Alloc vs boxed* < 1 means it allocated less. This is a near-best-case workload (value-type TIMESTAMP key over large sorted partitions); string keys and small partitions see less.

