-- feldera-fraud.sql — Feldera-side pipeline for the fraud-detection head-to-head.
--
-- This is the Feldera (Calcite SQL) counterpart of DbspNet's fraud benchmark
-- (src/DbspNet.Benchmarks/Fraud/FraudBenchmark.cs). It declares the same two input
-- tables and the same rolling-window feature view so both engines compute an
-- identical logical plan over identical data — the apples-to-apples basis for a
-- fraud comparison.
--
-- WHY THIS WORKLOAD: it is the ONLY benchmark that exercises DbspNet's
-- PARTITION BY window-aggregate operator (rolling SUM/COUNT OVER a RANGE-INTERVAL
-- frame). None of the Nexmark queries do (they use TUMBLE / HOP / ROW_NUMBER), so
-- the fraud comparison is where the typed/parallel window-aggregate path shows up
-- against Feldera. Nexmark stays automated via scripts/compare-nexmark.sh.
--
-- ============================================================================
-- STATUS: authored to match DbspNet's view; NOT yet validated against a live
-- Feldera. Treat as a starting point — see the CAVEATS before trusting numbers.
-- ============================================================================
--
-- ---- HEAD-TO-HEAD PROCEDURE -------------------------------------------------
--
-- 1. Same data on both sides (the hard requirement). DbspNet generates the
--    customers/transactions in-process, seeded and deterministic
--    (FraudBenchmark.Build{Customers,Transactions}):
--      customers:    id = 0..N-1, name = 'cust-'||id, zip = CAST(10000 + id%90000 AS VARCHAR)
--      transactions: txn_id = 0..M-1, cust_id = rand(0..N) [seed 7],
--                    amount = rand(1..100000), ts = 1_700_000_000_000_000us + i*step,
--                    where step = (90 days) / M  (txns spread evenly across ~90 days,
--                    so the 1d / 7d / 30d frames genuinely differ).
--    Feed Feldera the *same* rows. The DbspNet harness can dump exactly the rows it
--    loads, as headerless CSV in table-column order:
--      dotnet run -c Release --project src/DbspNet.Benchmarks -- \
--          fraud-dump <outDir> <historyTxns> <customers>
--    → <outDir>/customers.csv     (id,name,zip)
--      <outDir>/transactions.csv  (txn_id,cust_id,amount,ts ; ts = 'yyyy-MM-dd HH:mm:ss.ffffff' UTC)
--    Use the SAME <historyTxns> <customers> you pass to `fraud` so the benchmarked
--    and ingested data match. Load the two files into the tables below via the CSV
--    connector appropriate to your Feldera deployment.
--
-- 2. Match the metric. DbspNet reports two numbers (docs/benchmarks-fraud.md):
--      * per-event incremental latency — load the history, then time ONE
--        Insert+Step (the per-swipe fraud-scoring latency, DbspNet's headline), and
--      * sustained micro-batched throughput (events/s).
--    Measure the same on Feldera: steady-state per-input latency for a single new
--    transaction, and throughput for a batched replay. Exclude pipeline/circuit
--    construction from the timer on both sides (as the Nexmark harness does).
--
-- 3. Match the core count (Feldera workers == DbspNet W) and run on the same host,
--    same as scripts/compare-nexmark.sh.
--
-- ---- CAVEATS (validate before trusting numbers) -----------------------------
--
-- * LATENESS / unbounded state. DbspNet's `transactions` has NO lateness, so its
--   window state is unbounded (exact, grows with history — the per-event latency
--   drift the doc notes). Feldera may require a watermark on the ORDER BY column
--   (ts) for a streaming RANGE-OVER window; if so, add a LATENESS and apply the
--   SAME bound to DbspNet (CREATE TABLE transactions (... ts TIMESTAMP NOT NULL
--   LATENESS INTERVAL '30' DAY)) so both GC identically. Otherwise the comparison
--   is exact-unbounded vs bounded-approximate — not apples-to-apples. See the
--   commented LATENESS variant below.
-- * TIMESTAMP precision. ts is microseconds since epoch in DbspNet; only the
--   relative spacing matters for the frames, but feed both sides identical values.
-- * Connectors. The CREATE TABLE statements below are connector-agnostic. Add the
--   WITH ('connectors' = …) ingestion appropriate to your Feldera deployment.
--
-- =============================================================================

CREATE TABLE customers (
    id   BIGINT  NOT NULL,
    name VARCHAR NOT NULL,
    zip  VARCHAR NOT NULL
);

CREATE TABLE transactions (
    txn_id  BIGINT    NOT NULL,
    cust_id BIGINT    NOT NULL,
    amount  BIGINT    NOT NULL,
    ts      TIMESTAMP NOT NULL
    -- LATENESS variant (uncomment on both sides if Feldera needs a watermark):
    -- , LATENESS ts INTERVAL '30' DAY
);

-- The fraud feature vector: per-customer rolling 1-day / 7-day / 30-day
-- transaction COUNT and SUM, computed off one join. Identical to DbspNet's
-- FraudBenchmark.FeatureSql (three distinct RANGE-INTERVAL frames feed off one
-- transactions ⋈ customers join).
CREATE VIEW fraud_features AS
SELECT
    t.txn_id,
    t.cust_id,
    c.zip,
    COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1'  DAY PRECEDING AND CURRENT ROW) AS cnt_1d,
    SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '1'  DAY PRECEDING AND CURRENT ROW) AS sum_1d,
    COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7'  DAY PRECEDING AND CURRENT ROW) AS cnt_7d,
    SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '7'  DAY PRECEDING AND CURRENT ROW) AS sum_7d,
    COUNT(*)      OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS cnt_30d,
    SUM(t.amount) OVER (PARTITION BY t.cust_id ORDER BY t.ts RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW) AS sum_30d
FROM transactions t
JOIN customers c ON t.cust_id = c.id;
