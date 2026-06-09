# DbspNet — spine + memtable evaluation

Stream: 1,000,000 Nexmark events, W=24, batch=10,000, median **step** of 3 run(s) after one warmup. Host: .NET 10.0.8, 24 cores. Every spine config's output is cross-checked against flat. **Step↑** is flat/config (> 1.0× = the config out-steps the flat default).

The candidate default is **spine·merge·staged** — `TraceFamily.Spine` with the merge probe and the per-tick memtable (capacity 8,192, §10). **spine·merge** (memtable off) is shown to isolate the memtable's contribution. q0–q2 are stateless controls (no spine trace).

## Per-query — flat vs spine·merge vs spine·merge·staged (batch 10,000)

| Query | Description | flat step (ms) | spine·merge ↑ | spine·merge·staged ↑ |
|:------|:------------|---------------:|--------------:|---------------------:|
| q0 | passthrough — SELECT * FROM bid | 32.8 | 1.06× | 0.98× |
| q1 | currency conversion — map a column | 70.0 | 1.61× | 1.13× |
| q2 | selection — WHERE auction % 123 = 0 | 44.5 | 1.37× | 1.37× |
| q3 | local item suggestion — auction ⋈ person, filtered | 47.7 | 0.96× | 1.48× |
| q4 | average closing price by category | 580.4 | 0.39× | 0.80× |
| q9 | winning bids — top bid per auction | 338.0 | 0.99× | 1.03× |

## Memtable capacity sweep — q4 (batch 10,000)

| Capacity (keys) | spine·merge·staged step (ms) | Step↑ vs flat |
|----------------:|-----------------------------:|--------------:|
| flat (baseline) | 610.9 | 1.00× |
| 0 (memtable off) | 1550.8 | 0.39× |
| 1,024 | 892.8 | 0.68× |
| 4,096 | 750.6 | 0.81× |
| 8,192 | 665.7 | 0.92× |
| 16,384 | 712.7 | 0.86× |
| 32,768 | 746.4 | 0.82× |
| 65,536 | 741.7 | 0.82× |

## Verdict

Read the **stateful** queries (q3/q4/q9) at *spine·merge·staged vs flat*: that is the flip decision. The memtable's contribution is *spine·merge·staged vs spine·merge*. The controls (q0–q2) should sit at ~1.0× — any large deviation there is a spine-compile-path boundary cost unrelated to the trace. The capacity sweep locates the flush threshold that maximises q4 step throughput; too small re-introduces per-tick builds, too large grows the memtable's per-read merge cost.

## Conclusion (this run)

- **The memtable is an unconditional win for spine.** The q4 capacity sweep (the
  most reliable signal — same query, flat and staged measured back-to-back) takes
  the spine step from **0.39× (memtable off) to 0.92× at capacity 8,192**, a 2.4×
  speedup, with a clean knee: smaller (1,024→0.68×) re-introduces per-tick builds,
  larger (65,536→0.82×) grows the per-read memtable merge. **8,192 is the default
  capacity.** Per-query, the memtable also lifts q3 to ~1.48× and q9 to ~1.03×.
- **Do not flip the global flat→spine default.** Even with the memtable, q4 (the
  aggregate-heavy canonical case) is still ~0.8–0.92× flat, and the
  simpler/stateless queries gain nothing from spine. Flat stays the safe universal
  default.
- **But spine+memtable is now competitive** (~0.9–1.5× on the stateful queries,
  up from the pre-memtable 0.39–0.99×) — worth selecting when its spill / snapshot
  / bounded-memory properties matter, now at near-flat throughput.
- **Noise caveat.** The stateless controls calibrate it: q1/q2 hold no trace, so
  `merge` and `staged` are identical work, yet q1 reads 1.61× vs 1.13× — a ~±0.5×
  parallel-bench noise floor. Trust the sequential capacity sweep over individual
  per-query cells.

