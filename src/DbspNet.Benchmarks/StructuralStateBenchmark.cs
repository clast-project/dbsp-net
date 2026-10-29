// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks;

/// <summary>
/// <c>structstate [rowsScale]</c> — deterministic state-size instrument for the
/// structural program path (docs/design-state-representation.md round 3). Each shape
/// mirrors an ivm-bench view; each arm flips one state-representation switch. Reports
/// live bytes after a forced full GC (state retained by the compiled program) and bytes
/// allocated on this thread during push + step. No wall-clock.
/// </summary>
internal static class StructuralStateBenchmark
{
    private sealed record Arm(string Name, Action Setup, bool Typed = false);

    public static int Run(string[] args)
    {
        var scale = args.Length > 1 ? double.Parse(args[1], System.Globalization.CultureInfo.InvariantCulture) : 1.0;
        var only = args.Length > 2 ? args[2] : null;
        Console.WriteLine($"# structstate (scale {scale}) — live = bytes retained after full GC; alloc = bytes allocated in push+step");
        Console.WriteLine();
        Console.WriteLine("| shape | arm | rows in | view rows | live bytes | live B/row in | alloc bytes |");
        Console.WriteLine("|---|---|---:|---:|---:|---:|---:|");

        var arms = new List<Arm>
        {
            new("structural, MIN/MAX legacy (pre-round-3)", () =>
            {
                Reset();
                StructuralCompactAggregatorStateMode.MinMax = StructuralMinMaxStateKind.Legacy;
            }),
            new("structural, typed B/B2/G switches OFF", () =>
            {
                Reset();
                StructuralCompactAggregatorStateMode.MinMax = StructuralMinMaxStateKind.Legacy;
                CompactAggregatorStateMode.MinMax = MinMaxStateKind.Legacy;
                CompactAggregatorStateMode.CompactDistinct = false;
                LinearAggregateMode.Enabled = false;
            }),
            new("structural, B2 hybrid (prototype)", () =>
            {
                Reset();
                StructuralCompactAggregatorStateMode.MinMax = StructuralMinMaxStateKind.Hybrid;
            }),
            new("typed program view (B/B2/G on)", Reset, Typed: true),
            new("typed program view (B/B2/G off)", () =>
            {
                Reset();
                CompactAggregatorStateMode.MinMax = MinMaxStateKind.Legacy;
                CompactAggregatorStateMode.CompactDistinct = false;
                LinearAggregateMode.Enabled = false;
            }, Typed: true),
        };

        // Warm-up (JIT, statics) so the first measured arm is not charged for them.
        Measure(Shapes(0.01).First(), typed: false);
        Measure(Shapes(0.01).First(), typed: true);

        foreach (var shape in Shapes(scale))
        {
            if (only is not null && !shape.Name.StartsWith(only, StringComparison.Ordinal)) continue;
            var shapeArms = shape.Name.StartsWith("A-", StringComparison.Ordinal)
                ? new List<Arm>
                {
                    new("structural, TOP-K compact (default)", () => { Reset(); PartitionedTopKStateMode.Override = PartitionedTopKState.Compact; }),
                    new("structural, TOP-K legacy", () => { Reset(); PartitionedTopKStateMode.Override = PartitionedTopKState.Legacy; }),
                }
                : arms;
            foreach (var arm in shapeArms)
            {
                arm.Setup();
                var (rowsIn, viewRows, live, alloc) = Measure(shape, arm.Typed);
                Console.WriteLine($"| {shape.Name} | {arm.Name} | {rowsIn:N0} | {viewRows:N0} | {live:N0} | {(double)live / rowsIn:F0} | {alloc:N0} |");
            }
        }

        Reset();
        return 0;
    }

    private static void Reset()
    {
        CompactAggregatorStateMode.MinMax = CompactAggregatorStateMode.MinMaxDefault;
        CompactAggregatorStateMode.CompactDistinct = true;
        LinearAggregateMode.Enabled = true;
        PartitionedTopKStateMode.Override = null;
        StructuralCompactAggregatorStateMode.MinMax = StructuralCompactAggregatorStateMode.MinMaxDefault;
    }

    internal sealed record Shape(string Name, string[] Program, string Table, string View, Func<int, IEnumerable<object?[]>> Rows, int Ticks);

    private static (long RowsIn, int ViewRows, long Live, long Alloc) Measure(Shape shape, bool typed)
    {
        var outputs = new HashSet<string>(StringComparer.Ordinal) { shape.View };
        var program = SqlProgram.Compile(
            shape.Program, outputs,
            options: new CompileOptions { TypeEligibleProgramViews = typed },
            numericStringCoercion: true, nullCollation: NullCollation.Low);
        if (typed && PlanToCircuit.LastProgramTypedTally.Typed.Count == 0)
        {
            Console.WriteLine($"  (note: {shape.Name} did not compile typed: fell back to structural)");
        }

        var before = GC.GetTotalMemory(forceFullCollection: true);
        var table = program.Table(shape.Table);
        long rowsIn = 0;
        long alloc = 0;
        for (var t = 0; t < shape.Ticks; t++)
        {
            var batch = shape.Rows(t).Select(r => (r, 1L)).ToList();
            rowsIn += batch.Count;
            var a0 = GC.GetAllocatedBytesForCurrentThread();
            table.Push(batch);
            program.Step();
            alloc += GC.GetAllocatedBytesForCurrentThread() - a0;
        }

        var after = GC.GetTotalMemory(forceFullCollection: true);
        var viewRows = program.Outputs[shape.View].CurrentView.Count;
        GC.KeepAlive(program);
        return (rowsIn, viewRows, after - before, alloc);
    }

    // Deterministic LCG so every arm and every process sees the same rows.
    private static ulong Mix(ulong x)
    {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdUL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53UL;
        x ^= x >> 33;
        return x;
    }

    private static readonly Timestamp Epoch = Timestamp.Parse("2020-01-01 00:00:00");

    private static Timestamp Ts(long seconds) => new(Epoch.Microseconds + (seconds * 1_000_000L));

    internal static IEnumerable<Shape> Shapes(double scale)
    {
        int S(int n) => Math.Max(1, (int)(n * scale));

        // B2 — ivm-bench `watches`: GROUP BY many keys, MIN/MAX over CASE-filtered timestamps,
        // ~1.3 rows per group, append-only.
        var watchGroups = S(100_000);
        const int watchTicks = 10;
        yield return new Shape(
            "B2-watches",
            [
                "CREATE TABLE wh (customer_id BIGINT, symbol VARCHAR, company_id BIGINT, ts TIMESTAMP, action_type VARCHAR)",
                "CREATE VIEW w AS SELECT customer_id, symbol, company_id, " +
                "MIN(CASE WHEN action_type = 'Activate' THEN ts ELSE NULL END) AS placed, " +
                "MAX(CASE WHEN action_type = 'Cancelled' THEN ts ELSE NULL END) AS removed " +
                "FROM wh GROUP BY customer_id, symbol, company_id",
            ],
            "wh", "w",
            tick =>
            {
                var rows = new List<object?[]>();
                var per = watchGroups / watchTicks;
                for (var g = tick * per; g < (tick + 1) * per; g++)
                {
                    var h = Mix((ulong)g);
                    var cust = (long)(g / 3);
                    var sym = Utf8String.Of("SYM" + (g % 3 + (int)(h % 5000)));
                    var comp = (long)(h % 5000);
                    rows.Add([cust, sym, comp, Ts(g), Utf8String.Of("Activate")]);
                    if (h % 10 < 3)
                    {
                        rows.Add([cust, sym, comp, Ts(g + 86_400), Utf8String.Of("Cancelled")]);
                    }
                }

                return rows;
            },
            watchTicks);

        // B + G' — ivm-bench `broker_performance`'s broker_trades: few groups, many rows,
        // four COUNT(DISTINCT)s, nullable SUMs.
        var trades = S(200_000);
        const int tradeTicks = 10;
        IEnumerable<object?[]> TradeRows(int tick)
        {
            var per = trades / tradeTicks;
            for (var i = tick * per; i < (tick + 1) * per; i++)
            {
                var h = Mix((ulong)i + 77);
                yield return
                [
                    (long)(h % 100),
                    Utf8String.Of("C" + ((h >> 8) % 20_000)),
                    Utf8String.Of("S" + ((h >> 24) % 5_000)),
                    Utf8String.Of("A" + ((h >> 36) % 40_000)),
                    Ts(i * 60L),
                    (long)((h >> 4) % 1000),
                    (double)((h >> 12) % 10_000) / 100.0,
                    (double)((h >> 20) % 1000) / 100.0,
                ];
            }
        }

        const string tradeTable = "CREATE TABLE ft (broker BIGINT, cust VARCHAR NOT NULL, sec VARCHAR NOT NULL, " +
                                  "acct VARCHAR NOT NULL, ts TIMESTAMP, qty BIGINT, price DOUBLE, fee DOUBLE)";
        yield return new Shape(
            "B-broker",
            [
                tradeTable,
                "CREATE VIEW b AS SELECT broker, COUNT(*) AS n, COUNT(DISTINCT cust) AS uc, COUNT(DISTINCT sec) AS us, " +
                "COUNT(DISTINCT acct) AS ua, COUNT(DISTINCT CAST(ts AS DATE)) AS days, SUM(qty) AS vol, " +
                "SUM(price * qty) AS notional, SUM(fee) AS fees, COUNT(fee) AS nfee FROM ft GROUP BY broker",
            ],
            "ft", "b", TradeRows, tradeTicks);

        // Reference: same columns reach the trace, but COUNT(x) in place of COUNT(DISTINCT x)
        // — isolates the distinct sub-state from the trace.
        yield return new Shape(
            "B-broker-nodistinct",
            [
                tradeTable,
                "CREATE VIEW b AS SELECT broker, COUNT(*) AS n, COUNT(cust) AS uc, COUNT(sec) AS us, " +
                "COUNT(acct) AS ua, COUNT(CAST(ts AS DATE)) AS days, SUM(qty) AS vol, " +
                "SUM(price * qty) AS notional, SUM(fee) AS fees, COUNT(fee) AS nfee FROM ft GROUP BY broker",
            ],
            "ft", "b", TradeRows, tradeTicks);

        // G' — ivm-bench `fact_cash_balances`: many groups, one nullable SUM.
        var cash = S(200_000);
        const int cashTicks = 10;
        yield return new Shape(
            "G-cashbal",
            [
                "CREATE TABLE fct (cust BIGINT, acct BIGINT, d DATE, descr VARCHAR, amount DOUBLE)",
                "CREATE VIEW c AS SELECT cust, acct, d, SUM(amount) AS amount, descr FROM fct GROUP BY cust, acct, d, descr",
            ],
            "fct", "c",
            tick =>
            {
                var rows = new List<object?[]>();
                var per = cash / cashTicks;
                for (var i = tick * per; i < (tick + 1) * per; i++)
                {
                    var h = Mix((ulong)i + 991);
                    var acct = (long)(h % 50_000);
                    rows.Add(
                    [
                        acct / 2, acct, new Date32((int)(18_000 + ((h >> 20) % 1000))),
                        Utf8String.Of("descr " + ((h >> 40) % 50)), (double)((h >> 8) % 100_000) / 100.0,
                    ]);
                }

                return rows;
            },
            cashTicks);

        // G (strict) — same as above with NOT NULL amount: what the typed rules would accept.
        yield return new Shape(
            "G-cashbal-notnull",
            [
                "CREATE TABLE fct (cust BIGINT NOT NULL, acct BIGINT NOT NULL, d DATE NOT NULL, descr VARCHAR NOT NULL, amount DOUBLE NOT NULL)",
                "CREATE VIEW c AS SELECT cust, acct, d, SUM(amount) AS amount, descr FROM fct GROUP BY cust, acct, d, descr",
            ],
            "fct", "c",
            tick =>
            {
                var rows = new List<object?[]>();
                var per = cash / cashTicks;
                for (var i = tick * per; i < (tick + 1) * per; i++)
                {
                    var h = Mix((ulong)i + 991);
                    var acct = (long)(h % 50_000);
                    rows.Add(
                    [
                        acct / 2, acct, new Date32((int)(18_000 + ((h >> 20) % 1000))),
                        Utf8String.Of("descr " + ((h >> 40) % 50)), (double)((h >> 8) % 100_000) / 100.0,
                    ]);
                }

                return rows;
            },
            cashTicks);

        // A — partitioned TOP-1 dedup (absent from ivm-bench; included to size A on the
        // structural path anyway).
        var dedup = S(200_000);
        yield return new Shape(
            "A-dedup",
            [
                "CREATE TABLE e (k BIGINT NOT NULL, ts TIMESTAMP NOT NULL, v VARCHAR)",
                "CREATE VIEW d AS SELECT k, ts, v FROM (SELECT k, ts, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) AS rn FROM e) x WHERE rn <= 1",
            ],
            "e", "d",
            tick =>
            {
                var rows = new List<object?[]>();
                var per = dedup / 10;
                for (var i = tick * per; i < (tick + 1) * per; i++)
                {
                    var h = Mix((ulong)i + 5);
                    rows.Add([(long)(h % 100_000), Ts(i), Utf8String.Of("v" + (h % 997))]);
                }

                return rows;
            },
            10);
    }
}
