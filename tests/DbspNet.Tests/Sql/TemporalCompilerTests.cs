// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class TemporalCompilerTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    private static long WeightOf(ZSet<StructuralRow, Z64> z, params object?[] row) =>
        z.WeightOf(new StructuralRow(SqlTestHelpers.EncodeStrings(row))).Value;

    // ---- DDL ----

    [Fact]
    public void CreateTable_AcceptsTemporalColumnTypes()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(
            "CREATE TABLE t (d DATE NOT NULL, tm TIME NOT NULL, ts TIMESTAMP NOT NULL)"));

        var schema = catalog.Get("t");
        Assert.IsType<SqlDateType>(schema[0].Type);
        Assert.IsType<SqlTimeType>(schema[1].Type);
        Assert.IsType<SqlTimestampType>(schema[2].Type);
        Assert.Equal(typeof(Date32), schema[0].Type.ClrType);
        Assert.Equal(typeof(Time64), schema[1].Type.ClrType);
        Assert.Equal(typeof(Timestamp), schema[2].Type.ClrType);
    }

    // ---- Filter on DATE ----

    [Fact]
    public void Filter_ByDateComparison()
    {
        var q = Compile(
            ["CREATE TABLE events (id INT NOT NULL, d DATE NOT NULL)"],
            "SELECT id FROM events WHERE d >= CAST('2026-04-27' AS DATE)");

        q.Table("events").Insert(1, Date32.Parse("2026-04-26"));
        q.Table("events").Insert(2, Date32.Parse("2026-04-27"));
        q.Table("events").Insert(3, Date32.Parse("2026-04-28"));
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 2));
        Assert.Equal(1, WeightOf(q.Current, 3));
    }

    // ---- Equality on TIMESTAMP ----

    [Fact]
    public void Filter_ByTimestampEquality()
    {
        var q = Compile(
            ["CREATE TABLE events (id INT NOT NULL, ts TIMESTAMP NOT NULL)"],
            "SELECT id FROM events WHERE ts = CAST('2026-04-27 12:00:00' AS TIMESTAMP)");

        q.Table("events").Insert(1, Timestamp.Parse("2026-04-27 12:00:00"));
        q.Table("events").Insert(2, Timestamp.Parse("2026-04-27 12:00:01"));
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, 1));
    }

    // ---- GROUP BY date ----

    [Fact]
    public void GroupBy_OnDate()
    {
        var q = Compile(
            ["CREATE TABLE sales (d DATE NOT NULL, amount INT NOT NULL)"],
            "SELECT d, SUM(amount) AS total FROM sales GROUP BY d");

        var d1 = Date32.Parse("2026-04-26");
        var d2 = Date32.Parse("2026-04-27");
        q.Table("sales").Insert(d1, 10);
        q.Table("sales").Insert(d1, 20);
        q.Table("sales").Insert(d2, 100);
        q.Step();

        Assert.Equal(2, q.Current.Count);
        Assert.Equal(1, WeightOf(q.Current, d1, 30L));
        Assert.Equal(1, WeightOf(q.Current, d2, 100L));
    }

    // ---- Comparison rejects cross-temporal-kind ----

    [Fact]
    public void Compare_DateWithTimestamp_RejectedByResolver()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (d DATE NOT NULL, ts TIMESTAMP NOT NULL)"],
            "SELECT 1 FROM t WHERE d = ts"));
        Assert.Contains("not comparable", ex.Message);
    }

    [Fact]
    public void Compare_DateWithInt_RejectedByResolver()
    {
        var ex = Assert.Throws<ResolveException>(() => Compile(
            ["CREATE TABLE t (d DATE NOT NULL, n INT NOT NULL)"],
            "SELECT 1 FROM t WHERE d = n"));
        Assert.Contains("not comparable", ex.Message);
    }

    // ---- CAST round-trips through string ----

    [Fact]
    public void Cast_DateToStringAndBack()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL)"],
            "SELECT CAST(d AS VARCHAR) AS s FROM t");

        q.Table("t").Insert(Date32.Parse("2026-04-27"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "2026-04-27"));
    }

    [Fact]
    public void Cast_TimestampToStringAndBack()
    {
        var q = Compile(
            ["CREATE TABLE t (ts TIMESTAMP NOT NULL)"],
            "SELECT CAST(ts AS VARCHAR) AS s FROM t");

        q.Table("t").Insert(Timestamp.Parse("2026-04-27 12:00:00"));
        q.Step();

        Assert.Equal(1, WeightOf(q.Current, "2026-04-27 12:00:00.000000"));
    }

    // ---- Codec smoke test: rows with temporal columns are equatable ----

    [Fact]
    public void Codec_TemporalRowsKeyByValue()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL, ts TIMESTAMP NOT NULL)"],
            "SELECT d, ts FROM t");

        var d = Date32.Parse("2026-04-27");
        var ts = Timestamp.Parse("2026-04-27 12:00:00");
        q.Table("t").Insert(d, ts);
        q.Table("t").Insert(d, ts);  // same row twice — weight 2
        q.Step();

        Assert.Equal(1, q.Current.Count);
        Assert.Equal(2, WeightOf(q.Current, d, ts));
    }
}
