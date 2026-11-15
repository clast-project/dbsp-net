// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using DbspNet.Sql.Compiler;
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// <see cref="NumericDialect"/>: the two implementation-defined numeric behaviours the
/// TPC-DI re-validation against Feldera turned up (issue #6). Each case runs on the
/// structural compiler and, with <see cref="CompileOptions.TypeEligibleProgramViews"/>,
/// on the typed one. Both read the flag the resolver stamped on the node.
/// </summary>
public class NumericDialectTests
{
    public static TheoryData<bool> BothPaths => new() { false, true };

    private static object? EvalOne(
        string ddl, string select, object?[] row, NumericDialect dialect, bool typed)
    {
        var prog = SqlProgram.Compile(
            [ddl, $"CREATE VIEW v AS {select}"],
            new HashSet<string>(StringComparer.Ordinal) { "v" },
            options: new CompileOptions { TypeEligibleProgramViews = typed },
            numericDialect: dialect);
        prog.Table("t").Insert(row);
        prog.Step();
        var live = prog.Outputs["v"].CurrentView.Where(e => e.Value.Value != 0).ToList();
        Assert.Single(live);
        return live[0].Key[0];
    }

    // ---- CAST(numeric AS TIMESTAMP): microseconds by default, milliseconds under Feldera ----

    [Theory, MemberData(nameof(BothPaths))]
    public void EpochCast_Default_ReadsMicroseconds(bool typed)
    {
        var ts = Assert.IsType<Timestamp>(EvalOne(
            "CREATE TABLE t (n BIGINT NOT NULL)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [6440L], NumericDialect.Default, typed));
        Assert.Equal(6440L, ts.Microseconds);
    }

    [Theory, MemberData(nameof(BothPaths))]
    public void EpochCast_Feldera_ReadsMilliseconds(bool typed)
    {
        // The ivm-bench shape: crm_customer_mgmt's CAST(cdc_dsn AS TIMESTAMP) feeds
        // dim_account / dim_customer effective_timestamp. Feldera gives 00:00:06.44.
        var ts = Assert.IsType<Timestamp>(EvalOne(
            "CREATE TABLE t (n BIGINT NOT NULL)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [6440L], NumericDialect.Feldera, typed));
        Assert.Equal(6_440_000L, ts.Microseconds);
    }

    [Theory, MemberData(nameof(BothPaths))]
    public void EpochCast_Feldera_IntegerAndNullable(bool typed)
    {
        var ts = Assert.IsType<Timestamp>(EvalOne(
            "CREATE TABLE t (n INT)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [-1500], NumericDialect.Feldera, typed));
        Assert.Equal(-1_500_000L, ts.Microseconds);
        Assert.Null(EvalOne(
            "CREATE TABLE t (n INT)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [null], NumericDialect.Feldera, typed));
    }

    [Theory, MemberData(nameof(BothPaths))]
    public void EpochCast_Feldera_DoubleRoundsHalfEvenToTheMicrosecond(bool typed)
    {
        // Feldera: (millis * 1000.0).round_ties_even(). 0.0025 ms is 2.5 µs and goes to 2;
        // 0.0035 ms is 3.5 µs and goes to 4. Both midpoints are exact in binary after the
        // multiply, so this pins the tie rule rather than float noise.
        Assert.Equal(2L, Assert.IsType<Timestamp>(EvalOne(
            "CREATE TABLE t (n DOUBLE NOT NULL)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [0.0025], NumericDialect.Feldera, typed)).Microseconds);
        Assert.Equal(1_500L, Assert.IsType<Timestamp>(EvalOne(
            "CREATE TABLE t (n DOUBLE NOT NULL)", "SELECT CAST(n AS TIMESTAMP) FROM t",
            [1.5], NumericDialect.Feldera, typed)).Microseconds);
    }

    // ---- DECIMAL division: half-even by default, toward zero under Feldera ----

    private const string DivDdl = "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL)";
    private const string DivSelect = "SELECT CAST(a AS DECIMAL(10,0)) / CAST(b AS DECIMAL(10,0)) FROM t";

    // The quotient's digits as a string: its scale comes from the promotion rules, so the
    // assertions check the digit pattern (repeating 6s, then a rounded 7 or a truncated 6).
    private static string Digits(object? v) => Assert.IsType<Decimal128>(v).Mantissa.ToString(CultureInfo.InvariantCulture);

    [Theory, MemberData(nameof(BothPaths))]
    public void DecimalDivide_Default_RoundsHalfEven(bool typed)
    {
        Assert.Matches("^6+7$", Digits(EvalOne(DivDdl, DivSelect, [2L, 3L], NumericDialect.Default, typed)));
        Assert.Matches("^-6+7$", Digits(EvalOne(DivDdl, DivSelect, [-2L, 3L], NumericDialect.Default, typed)));
    }

    [Theory, MemberData(nameof(BothPaths))]
    public void DecimalDivide_Feldera_TruncatesTowardZero(bool typed)
    {
        Assert.Matches("^6+$", Digits(EvalOne(DivDdl, DivSelect, [2L, 3L], NumericDialect.Feldera, typed)));
        Assert.Matches("^-6+$", Digits(EvalOne(DivDdl, DivSelect, [-2L, 3L], NumericDialect.Feldera, typed)));
    }

    [Theory, MemberData(nameof(BothPaths))]
    public void DecimalDivide_IvmBenchPctMarketVolume(bool typed)
    {
        // market_volatility.pct_market_volume for symbol AAAAAAAAAAAABBK at SF=3:
        // ROUND(total_volume * 100.0 / mkt_total_volume, 4). The exact quotient is
        // 0.058449981…, so the right answer is 0.0584 (Feldera's). Half-even division to the
        // scale-6 result gives 0.058450, which ROUND then takes to 0.0585.
        const string ddl = "CREATE TABLE t (v BIGINT NOT NULL, total BIGINT NOT NULL)";
        const string select = "SELECT ROUND(v * 100.0 / NULLIF(total, 0), 4) FROM t";
        object?[] row = [375_022_299_280L, 641_612_352_619_912L];
        // Mantissas at the result's scale 6 (the parquet column is DECIMAL(38,6)).
        Assert.Equal("58400", Digits(EvalOne(ddl, select, row, NumericDialect.Feldera, typed)));
        Assert.Equal("58500", Digits(EvalOne(ddl, select, row, NumericDialect.Default, typed)));
    }

    [Fact]
    public void DecimalDivide_Feldera_ZeroDivisorStillThrows()
    {
        var prog = SqlProgram.Compile(
            [DivDdl, $"CREATE VIEW v AS {DivSelect}"],
            new HashSet<string>(StringComparer.Ordinal) { "v" },
            numericDialect: NumericDialect.Feldera);
        prog.Table("t").Insert(1L, 0L);
        Assert.ThrowsAny<DivideByZeroException>(() => prog.Step());
    }

    [Fact]
    public void Dialect_SurvivesJoinRewrites()
    {
        // The flag lives on the resolved node, so the rewriters must carry it. A filter on
        // the right side of a join is pushed into that input through ShiftColumnIndices,
        // and join column pruning remaps the projection's indices. Under Feldera, 2/3
        // truncates to 0.666…6 and the row qualifies. If either rewrite dropped the flag, the
        // half-even quotient (0.666…7) would fail the filter and the view would be empty.
        var prog = SqlProgram.Compile(
            [
                "CREATE TABLE l (k INT NOT NULL, pad VARCHAR)",
                "CREATE TABLE r (k INT NOT NULL, a BIGINT NOT NULL, b BIGINT NOT NULL, n BIGINT NOT NULL)",
                "CREATE VIEW v AS SELECT CAST(r.a AS DECIMAL(10,0)) / r.b AS q, CAST(r.n AS TIMESTAMP) AS ts " +
                "FROM l JOIN r ON l.k = r.k WHERE CAST(r.a AS DECIMAL(10,0)) / r.b < 0.6666666666665 " +
                "AND CAST(r.n AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:06.44'",
            ],
            new HashSet<string>(StringComparer.Ordinal) { "v" },
            numericDialect: NumericDialect.Feldera);
        prog.Table("l").Insert(1, "x");
        prog.Table("r").Insert(1, 2L, 3L, 6440L);
        prog.Step();
        var live = prog.Outputs["v"].CurrentView.Where(e => e.Value.Value != 0).ToList();
        var row = Assert.Single(live).Key;
        Assert.Matches("^6+$", Digits(row[0]));
        Assert.Equal(6_440_000L, Assert.IsType<Timestamp>(row[1]).Microseconds);
    }

    [Fact]
    public void Dialect_SurvivesFilterPushdownThroughProjection()
    {
        // Pushing the outer WHERE below the derived table's projection rebuilds the
        // predicate's own division and cast nodes (SubstituteViaProjection). Each must keep
        // its flag, or the row is filtered out.
        var prog = SqlProgram.Compile(
            [
                "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, n BIGINT NOT NULL)",
                "CREATE VIEW v AS SELECT b FROM (SELECT CAST(a AS DECIMAL(10,0)) AS da, b, n FROM t) s " +
                "WHERE da / b < 0.6666666666665 AND CAST(n AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:06.44'",
            ],
            new HashSet<string>(StringComparer.Ordinal) { "v" },
            numericDialect: NumericDialect.Feldera);
        prog.Table("t").Insert(2L, 3L, 6440L);
        prog.Step();
        var live = prog.Outputs["v"].CurrentView.Where(e => e.Value.Value != 0).ToList();
        Assert.Equal(3L, Assert.Single(live).Key[0]);
    }

    [Fact]
    public void Default_IsTheDefaultWhenUnspecified()
    {
        var prog = SqlProgram.Compile(
            [DivDdl, $"CREATE VIEW v AS {DivSelect}"],
            new HashSet<string>(StringComparer.Ordinal) { "v" });
        prog.Table("t").Insert(2L, 3L);
        prog.Step();
        Assert.Matches("^6+7$", Digits(Assert.Single(prog.Outputs["v"].CurrentView).Key[0]));
    }
}
