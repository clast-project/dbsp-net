// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// §5.36 (docs/design-bounded-memory.md): a CONCAT_WS with a literal separator writes its arguments' text into
// one buffer, casts to text formatting straight into it, and MD5 over it hashes the buffer. Required: the
// same rows as the runtime-dispatched form for every argument shape (casts of each type, safe and not,
// COALESCE, literals, NULLs, nesting, a failing TRY_CAST), and far less allocation for the dbt surrogate key.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Expressions;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class FusedConcatWsTests
{
    private const string Ddl =
        "CREATE TABLE t (i INT, l BIGINT, d DOUBLE, r REAL, b BOOLEAN, dt DATE, s VARCHAR, ts TIMESTAMP, m DECIMAL(10, 2))";

    private const string AllShapes =
        "CONCAT_WS('-', COALESCE(TRY_CAST(i AS TEXT), ''), COALESCE(TRY_CAST(l AS TEXT), ''), TRY_CAST(d AS TEXT), " +
        "CAST(r AS TEXT), CAST(b AS TEXT), TRY_CAST(dt AS TEXT), s, CAST(ts AS TEXT), CAST(m AS TEXT), 'lit', " +
        "COALESCE(s, NULL, 'x'), COALESCE(TRY_CAST(i AS TEXT), TRY_CAST(l AS TEXT), 'none'), " +
        "CONCAT_WS('|', s, CAST(i AS TEXT)), UPPER(s))";

    public static TheoryData<string> Queries => new()
    {
        "SELECT i, " + AllShapes + " AS j FROM t",
        "SELECT i, MD5(" + AllShapes + ") AS h FROM t",
        "SELECT i, CONCAT_WS('', s, s) AS j, CONCAT_WS(', ', CAST(NULL AS VARCHAR), s) AS k FROM t",
        "SELECT i, MD5(CONCAT_WS(s, s, CAST(i AS TEXT))) AS h FROM t", // a column separator: not fused
    };

    private static CompiledQuery Compile(string query, bool fused)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        var saved = BuiltinScalarFunctions.FusedConcatWs;
        BuiltinScalarFunctions.FusedConcatWs = fused;
        try
        {
            return PlanToCircuit.Compile(plan);
        }
        finally
        {
            BuiltinScalarFunctions.FusedConcatWs = saved;
        }
    }

    private static readonly object?[] Ints = [null, 0, -1, 42, int.MinValue, int.MaxValue];
    private static readonly object?[] Longs = [null, 0L, long.MinValue, 1234567890123L];
    private static readonly object?[] Doubles = [null, 0.0, -0.0, 0.1, 1e300, -2.5e-8, double.NaN, double.PositiveInfinity, 1.0 / 3];
    private static readonly object?[] Reals = [null, 0f, 1.5f, float.NegativeInfinity, 3.4028235E+38f, 0.1f];
    private static readonly object?[] Bools = [null, true, false];
    private static readonly object?[] Dates = [null, new Date32(0), new Date32(-719162), new Date32(2932896), new Date32(20000), new Date32(int.MaxValue / 2)];
    private static readonly object?[] Strings = [null, "", "a", "héllo wörld", "x-y", "日本"];
    private static readonly object?[] Stamps = [null, Timestamp.Parse("2026-05-04 12:30:45.5"), Timestamp.Parse("1999-12-31 00:00:00")];
    private static readonly object?[] Decimals = [null, "1.25", "-0.10", "99999999.99"];

    private static void Fill(CompiledQuery q, int rows)
    {
        var rng = new Random(7);
        object? Pick(object?[] from) => from[rng.Next(from.Length)];
        for (var k = 0; k < rows; k++)
        {
            q.Table("t").Insert(k, Pick(Longs), Pick(Doubles), Pick(Reals), Pick(Bools), Pick(Dates), Pick(Strings), Pick(Stamps), Pick(Decimals));
        }
    }

    [Theory]
    [MemberData(nameof(Queries))]
    public void Fused_concat_ws_matches_the_runtime_form(string query)
    {
        var plain = Compile(query, fused: false);
        var fused = Compile(query, fused: true);
        Fill(plain, 600);
        Fill(fused, 600);
        plain.Step();
        fused.Step();
        Assert.Equal(600, plain.Current.Count);
        Assert.Equal(plain.Current, fused.Current);
    }

    // The out-of-range date: TRY_CAST makes it NULL in both forms (COALESCE then takes its fallback).
    [Fact]
    public void A_failing_try_cast_is_null_in_the_buffer_too()
    {
        const string query = "SELECT CONCAT_WS('-', 'a', COALESCE(TRY_CAST(dt AS TEXT), 'bad'), 'b') AS j FROM t";
        foreach (var fused in new[] { false, true })
        {
            var q = Compile(query, fused);
            q.Table("t").Insert(1, null, null, null, null, new Date32(int.MaxValue / 2), null, null, null);
            q.Step();
            var row = Assert.Single(q.Current).Key;
            Assert.Equal("a-bad-b", ((Utf8String)row[0]!).ToStringDecoded());
        }
    }

    // The dbt surrogate key over eleven columns: casts formatted in the buffer and the concatenation hashed
    // there allocate a fraction of the casts' strings, their UTF-8 copies and the joined string.
    [Fact]
    public void A_surrogate_key_allocates_a_fraction_of_the_runtime_form()
    {
        const string query =
            "SELECT MD5(CONCAT_WS('-', COALESCE(TRY_CAST(i AS TEXT), ''), COALESCE(TRY_CAST(l AS TEXT), ''), " +
            "COALESCE(TRY_CAST(dt AS TEXT), ''), COALESCE(TRY_CAST(i AS TEXT), ''), COALESCE(TRY_CAST(l AS TEXT), ''), " +
            "COALESCE(TRY_CAST(i AS TEXT), ''), COALESCE(TRY_CAST(dt AS TEXT), ''), COALESCE(TRY_CAST(s AS TEXT), ''), " +
            "COALESCE(TRY_CAST(s AS TEXT), ''), COALESCE(TRY_CAST(i AS TEXT), ''), COALESCE(TRY_CAST(l AS TEXT), ''))) AS k FROM t";
        long Measure(bool fused)
        {
            // Managed deltas whatever the suite forces: the measure is the expression's, not block encoding's.
            using var _ = DbspNet.Core.Operators.Stateful.Blocks.SpillSettings.UseStepSpill(null);
            var q = Compile(query, fused);
            for (var k = 0; k < 20_000; k++)
            {
                q.Table("t").Insert(k, 1_000_000L + k, null, null, null, new Date32(19_000 + (k % 365)), "store", null, null);
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            q.Step();
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        _ = Measure(true); // JIT
        var plain = Measure(false);
        var fusedBytes = Measure(true);
        Console.WriteLine($"SURROGATE plain={plain} fused={fusedBytes} ratio={(double)fusedBytes / plain:F2}");
        Assert.True(fusedBytes * 2 < plain, $"fused {fusedBytes} B, runtime form {plain} B");
    }
}
