# DbspNet — narrow-key partitioned TOP-K crossover (§22.7)

Where does the §22 narrow `{order, wideRow}` key overtake whole-row keying as the TOP-K `limit` grows? §22.6 measured only limit∈{1,10}; this sweeps the real single-circuit W=1 path (cached `StructuralRow` hash and all) at intermediate limits on both partition shapes, to pick the cheap static gate predicate.

Stream: 1,000,000 events (920,000 bids), batch=10,000, W=1, median ns/event of 5 run(s) after one warmup; allocation is one dedicated primed pass. `time↑` = whole-row ÷ narrow (>1 ⇒ narrow faster); `alloc↑` likewise on bytes/event. `Auto pick` is what the limit gate (`limit > 1`) selects.

## q18 (PARTITION BY bidder,auction — tiny partitions)

| limit | wholerow ns | narrow ns | time↑ | wholerow B | narrow B | alloc↑ | out rows | Auto pick |
|------:|------------:|----------:|------:|-----------:|---------:|-------:|---------:|:----------|
| 1 | 1918.5 | 1676.6 | 1.14× | 2176 | 2224 | 0.98× | 9,200 | wholerow |
| 2 | 1631.5 | 1640.2 | 0.99× | 2176 | 2224 | 0.98× | 9,200 | narrow |
| 3 | 1627.6 | 1773.9 | 0.92× | 2176 | 2224 | 0.98× | 9,200 | narrow |
| 5 | 1780.6 | 1831.2 | 0.97× | 2176 | 2224 | 0.98× | 9,200 | narrow |
| 10 | 1643.8 | 1688.5 | 0.97× | 2176 | 2224 | 0.98× | 9,200 | narrow |

## q19 (PARTITION BY auction — accumulating)

| limit | wholerow ns | narrow ns | time↑ | wholerow B | narrow B | alloc↑ | out rows | Auto pick |
|------:|------------:|----------:|------:|-----------:|---------:|-------:|---------:|:----------|
| 1 | 1802.8 | 1010.8 | 1.78× | 3224 | 1107 | 2.91× | 1,430 | wholerow |
| 2 | 1836.6 | 1097.5 | 1.67× | 3277 | 1159 | 2.83× | 2,755 | narrow |
| 3 | 1894.6 | 1189.7 | 1.59× | 3314 | 1197 | 2.77× | 3,908 | narrow |
| 5 | 2109.1 | 1392.8 | 1.51× | 3526 | 1416 | 2.49× | 5,904 | narrow |
| 10 | 2503.5 | 1689.0 | 1.48× | 3831 | 1734 | 2.21× | 8,706 | narrow |

**Reading it.** Narrow only pays where a partition accumulates state: on the accumulating (q19) shape the time/alloc win grows with the limit (more TOP-K window to hash, more `_accum` to compare); on the tiny-partition (q18) shape it stays flat-to-negative at every limit — confirming `limit` is a proxy for per-partition state that is only sound on accumulating partitions, which is why the gate is conservative (leaves all TOP-1 on whole-row).

