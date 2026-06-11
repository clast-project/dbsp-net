# DbspNet — exchange-gather whole-row-hash share (§22.8)

The exchange `gather` rebuilds the post-shuffle `IndexedZSet` by hashing each per-tick delta row into the inner per-key Z-set (`ExchangeIndexOp.Step`). That whole-row hash is the **only** width-attributable cost a shuffle-narrowing build could remove — `split` copies a row reference and hashes only the partition key (width-independent), `wait` is coordination, `op` is the TOP-K floor. This A/Bs the gather inner build with the wide 7-col bid row vs a narrow `{order, ref}` payload; the row is a value-type with an uncached structural hash (the typed W>1 path q18 runs).

Stream: 3,906 ticks × 256 rows/tick (~1,000,000 delta rows), median of 7 runs. `wide→narrow %` is the whole-row-hash share of gather — the shuffle-narrowing ceiling, before multiplying by gather's (small) share of step.

Two phases per shape: **gather** (the index-rebuild hash only) and **split+gather** (the full per-worker move — bucket each row into W=8 destinations by `hash(partitionKey)%W`, then rebuild). The typed path exchanges *value structs*, so `split` copies the whole payload by value into the bucket list — making it width-attributable too, not just gather.

| Shape | phase | wide ns/row | narrow ns/row | wide→narrow % | wide B/row | narrow B/row |
|:--|:--|--:|--:|--:|--:|--:|
| q18 (tiny partitions, ~1 row/key/tick) | gather | 348.0 | 147.0 | 58% | 527 | 455 |
| q18 (tiny partitions, ~1 row/key/tick) | split+gather | 358.0 | 97.0 | 73% | 527 | 455 |
| q19 (accumulating, several rows/key/tick) | gather | 351.0 | 173.0 | 51% | 527 | 455 |
| q19 (accumulating, several rows/key/tick) | split+gather | 378.0 | 118.0 | 69% | 527 | 455 |

**Reading it.** `wide→narrow %` is how much of that phase the narrow payload saves. Multiply the **split+gather** save by the movement share of step (`stepprofile`: q18 split+gather ≈ 25–37 % of step; the rest is `wait` coordination and `op`, the TOP-K per-row floor §22.6 measured flat to narrowing on q18's size-1 partitions). The lever cannot touch `op`, so its whole-query ceiling is `(split+gather save) × move%`.

