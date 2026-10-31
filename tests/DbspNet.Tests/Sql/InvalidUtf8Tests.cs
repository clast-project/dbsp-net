// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// What the engine does with VARCHAR bytes that are not valid UTF-8 (docs/design-connectors.md,
// G5, contract item 4). Nothing validates them on the way in, so these pin the "garbage in,
// garbage out" behaviour: no string function throws, and state stays consistent — equality,
// hashing, grouping, joins and snapshots all treat a VARCHAR as its bytes, so retracting the same
// bytes cancels exactly, on every path and across a restore. What garbage comes OUT is lossy only
// where a function decodes to a .NET string (LIKE / REGEXP_*), which substitutes U+FFFD.
using Apache.Arrow;
using DbspNet.Arrow;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class InvalidUtf8Tests
{
    private const string Ddl = "CREATE TABLE t (k BIGINT NOT NULL, s VARCHAR NOT NULL)";
    private const string Ddl2 = "CREATE TABLE u (j BIGINT NOT NULL, s VARCHAR NOT NULL)";

    /// <summary>Malformed sequences: lone continuation, truncated 2/3/4-byte, overlong, an encoded
    /// surrogate, a byte never valid in UTF-8, and each embedded among ASCII.</summary>
    public static readonly byte[][] Malformed =
    [
        [0x80],
        [0xC3],
        [0xE2, 0x82],
        [0x61, 0xF0, 0x9F],
        [0xC0, 0xAF],
        [0xED, 0xA0, 0x80],
        [0xFF, 0x61],
        [0x61, 0x62, 0x80, 0x61, 0x63],
    ];

    public static readonly string[] Functions =
    [
        "LENGTH(s)", "UPPER(s)", "LOWER(s)", "SUBSTRING(s, 1, 2)", "SUBSTRING(s, 2)", "TRIM(s)",
        "LTRIM(s)", "RTRIM(s)", "CONCAT(s, 'x')", "s || 'y'",
        "REPLACE(s, 'a', 'b')", "STRPOS(s, 'a')", "POSITION('a' IN s)", "SPLIT_PART(s, 'a', 1)",
        "SPLIT_INDEX(s, 'a', 0)", "MD5(s)", "REGEXP_REPLACE(s, 'a', 'b')",
        "CASE WHEN s LIKE '%a%' THEN 1 ELSE 0 END", "CASE WHEN s = 'a' THEN 1 ELSE 0 END",
    ];

    public static IEnumerable<object[]> FunctionCases() =>
        from f in Functions from i in Enumerable.Range(0, Malformed.Length) select new object[] { f, i };

    private static Utf8String Bad(int i) => Utf8String.FromBytes((byte[])Malformed[i].Clone());

    private static LogicalPlan Plan(string sql, params string[] ddl)
    {
        var resolver = new Resolver(new Catalog());
        foreach (var d in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(d));
        }

        return PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
    }

    [Theory]
    [MemberData(nameof(FunctionCases))]
    public void StringFunction_DoesNotThrow_OnEitherPath(string function, int input)
    {
        var plan = Plan($"SELECT k, {function} AS r FROM t", Ddl);

        var structural = PlanToCircuit.Compile(plan);
        structural.Table("t").Insert(1L, Bad(input));
        structural.Step();
        Assert.Single(structural.Current);

        if (TypedPlanCompiler.TryCompile(plan, out var typed))
        {
            typed!.Table("t").Insert(1L, Bad(input));
            typed.Step();
            Assert.Single(typed.Current);
        }
    }

    [Fact]
    public void Cast_ToBigint_FailsLikeAnyNonNumericString()
    {
        // GIGO: a cast that cannot parse fails the same way for malformed bytes as for 'abc'.
        var plan = Plan("SELECT CAST(s AS BIGINT) AS n FROM t", Ddl);
        var exNonNumeric = Record.Exception(() => Run(plan, Utf8String.Of("abc")));
        var exMalformed = Record.Exception(() => Run(plan, Bad(0)));
        Assert.NotNull(exNonNumeric);
        Assert.NotNull(exMalformed);
        Assert.Equal(exNonNumeric!.GetType(), exMalformed!.GetType());

        static void Run(LogicalPlan plan, Utf8String s)
        {
            var q = PlanToCircuit.Compile(plan);
            q.Table("t").Insert(1L, s);
            q.Step();
        }
    }

    public static readonly string[] StatefulQueries =
    [
        "SELECT s, COUNT(*) AS c FROM t GROUP BY s",
        "SELECT DISTINCT s FROM t",
        "SELECT t.k, u.j FROM t JOIN u ON t.s = u.s",
        "SELECT k, s FROM (SELECT k, s, ROW_NUMBER() OVER (PARTITION BY s ORDER BY k) AS rn FROM t) x WHERE rn = 1",
        "SELECT COUNT(DISTINCT s) AS c FROM t",
    ];

    public static IEnumerable<object[]> StatefulCases() =>
        from q in StatefulQueries select new object[] { q };

    [Theory]
    [MemberData(nameof(StatefulCases))]
    public void RetractingTheSameBytes_CancelsExactly(string sql)
    {
        // Inserted and retracted as distinct byte arrays with equal contents: the engine must
        // match them by bytes, and every distinct malformed value must stay distinct.
        var q = PlanToCircuit.Compile(Plan(sql, Ddl, Ddl2));
        var net = new Dictionary<StructuralRow, long>();
        for (var i = 0; i < Malformed.Length; i++)
        {
            q.Table("t").Insert((long)i, Bad(i));
            if (q.Inputs.ContainsKey("u"))
            {
                q.Table("u").Insert((long)i, Bad(i));
            }
        }

        q.Step();
        Accumulate(q, net);
        Assert.NotEmpty(net);
        // No two malformed values may collapse into one.
        if (sql.Contains("GROUP BY", StringComparison.Ordinal) || sql.StartsWith("SELECT DISTINCT", StringComparison.Ordinal))
        {
            Assert.Equal(Malformed.Length, net.Count);
        }
        else if (sql.Contains("COUNT(DISTINCT", StringComparison.Ordinal))
        {
            Assert.Equal((long)Malformed.Length, Assert.Single(net).Key[0]);
        }

        for (var i = 0; i < Malformed.Length; i++)
        {
            q.Table("t").Delete((long)i, Bad(i));
            if (q.Inputs.ContainsKey("u"))
            {
                q.Table("u").Delete((long)i, Bad(i));
            }
        }

        q.Step();
        Accumulate(q, net);
        Assert.Empty(net);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SnapshotRestore_KeepsTheBytes_SoALaterRetractionCancels(bool malformed)
    {
        const string Sql = "SELECT s, COUNT(*) AS c FROM t GROUP BY s";
        Utf8String Value(int i) => malformed ? Bad(i) : Utf8String.Of("v" + i);
        var fs = new InMemoryTableFileSystem();
        var producer = PlanToCircuit.Compile(Plan(Sql, Ddl), ArrowSqlSnapshotCodecs.Instance);
        var net = new Dictionary<StructuralRow, long>();
        for (var i = 0; i < Malformed.Length; i++)
        {
            producer.Table("t").Insert((long)i, Value(i));
        }

        producer.Step();
        Accumulate(producer, net);
        await Snapshot.WriteAsync(producer.Circuit, fs);

        var consumer = PlanToCircuit.Compile(Plan(Sql, Ddl), ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(consumer.Circuit, fs);
        for (var i = 0; i < Malformed.Length; i++)
        {
            consumer.Table("t").Delete((long)i, Value(i));
        }

        consumer.Step();
        Accumulate(consumer, net);
        Assert.Empty(net);
    }

    [Fact]
    public void ArrowIngestAndOutput_PreserveTheBytes()
    {
        // In through every StringDecoding mode, out through ToArrowDelta: the raw bytes survive.
        foreach (var mode in new[] { StringDecoding.Copy, StringDecoding.Arena, StringDecoding.Alias, StringDecoding.Transcode })
        {
            var q = PlanToCircuit.Compile(Plan("SELECT k, s FROM t", Ddl));
            var batch = Batch();
            q.Table("t").PushArrow(batch, mode);
            q.Step();
            var delta = q.ToArrowDelta();
            var output = (StringArray)delta.Rows.Column(1);
            var keys = (Int64Array)delta.Rows.Column(0);
            Assert.Equal(Malformed.Length, output.Length);
            for (var r = 0; r < output.Length; r++)
            {
                var i = (int)keys.GetValue(r)!.Value;
                var bytes = output.GetBytes(r).ToArray();
                if (mode == StringDecoding.Transcode)
                {
                    // The legacy round trip through a .NET string substitutes U+FFFD — lossy, but
                    // deterministic, so it still cannot split state.
                    Assert.Equal(System.Text.Encoding.UTF8.GetBytes(System.Text.Encoding.UTF8.GetString(Malformed[i])), bytes);
                }
                else
                {
                    Assert.Equal(Malformed[i], bytes);
                }
            }
        }

        static RecordBatch Batch()
        {
            var k = new Int64Array.Builder();
            var s = new StringArray.Builder();
            for (var i = 0; i < Malformed.Length; i++)
            {
                k.Append(i);
                s.Append(Malformed[i].AsSpan());
            }

            return new RecordBatch.Builder()
                .Append("k", false, k.Build())
                .Append("s", false, s.Build())
                .Build();
        }
    }

    private static void Accumulate(CompiledQuery q, Dictionary<StructuralRow, long> net)
    {
        foreach (var (row, w) in q.Current)
        {
            var n = net.GetValueOrDefault(row) + w.Value;
            if (n == 0)
            {
                net.Remove(row);
            }
            else
            {
                net[row] = n;
            }
        }
    }
}
