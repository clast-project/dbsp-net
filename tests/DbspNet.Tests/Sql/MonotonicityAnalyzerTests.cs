// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Sql.Parser.Ast;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Unit tests for the LATENESS monotonicity analyzer (Phase 3). Plans are built
/// directly (no parser yet) to exercise each propagation rule in isolation,
/// including the negative cases where monotonicity must be lost.
/// </summary>
public class MonotonicityAnalyzerTests
{
    private static readonly SqlType Bigint = new SqlBigintType(false);

    private static Schema Sch(params string[] names) =>
        new(names.Select(n => new SchemaColumn(n, Bigint)).ToList());

    private static ScanPlan Scan(string table, string[] cols, params int[] lateColumns) =>
        new(table, Sch(cols), lateColumns.ToDictionary(i => i, _ => 1000L));

    private static ResolvedColumn Col(int index) => new(index, Bigint);

    [Fact]
    public void Scan_DeclaredColumn_IsMonotoneWithItsSource()
    {
        var scan = Scan("t", new[] { "ts", "v" }, 0);
        var info = MonotonicityAnalyzer.Analyze(scan);

        Assert.True(info.IsMonotone(scan, 0));
        Assert.False(info.IsMonotone(scan, 1));
        var sources = info.Sources(scan, 0)!;
        Assert.Single(sources);
        Assert.Contains(new LatenessSource("t", 0), sources);
    }

    [Fact]
    public void Filter_PassesMonotonicityThrough()
    {
        var scan = Scan("t", new[] { "ts", "v" }, 0);
        var filter = new FilterPlan(scan, Col(1)); // predicate is irrelevant to monotonicity
        var info = MonotonicityAnalyzer.Analyze(filter);

        Assert.True(info.IsMonotone(filter, 0));
        Assert.False(info.IsMonotone(filter, 1));
    }

    [Fact]
    public void Project_PassThroughStaysMonotone_TransformingDoesNot()
    {
        var scan = Scan("t", new[] { "ts", "v" }, 0);
        var project = new ProjectPlan(
            scan,
            new[]
            {
                new ProjectionItem(Col(0), "ts"),                              // pass-through of monotone
                new ProjectionItem(new ResolvedCast(Col(0), Bigint), "tsCast"), // transforming → not
                new ProjectionItem(Col(1), "v"),                              // pass-through of non-monotone
            },
            Sch("ts", "tsCast", "v"));
        var info = MonotonicityAnalyzer.Analyze(project);

        Assert.True(info.IsMonotone(project, 0));
        Assert.False(info.IsMonotone(project, 1));
        Assert.False(info.IsMonotone(project, 2));
    }

    [Fact]
    public void InnerJoin_BothKeysMonotone_UnionSources_NonKeyLost()
    {
        var a = Scan("a", new[] { "k", "x" }, 0);
        var b = Scan("b", new[] { "k", "y" }, 0);
        var join = new JoinPlan(
            a, b, JoinType.Inner,
            new[] { new JoinEquality(0, 0, Bigint) },
            Residual: null,
            Schema: a.Schema.Concat(b.Schema)); // [a.k, a.x, b.k, b.y]
        var info = MonotonicityAnalyzer.Analyze(join);

        Assert.True(info.IsMonotone(join, 0));  // a.k
        Assert.True(info.IsMonotone(join, 2));  // b.k
        Assert.False(info.IsMonotone(join, 1)); // a.x (non-key)
        Assert.False(info.IsMonotone(join, 3)); // b.y (non-key)

        var sources = info.Sources(join, 0)!;
        Assert.Equal(2, sources.Count);
        Assert.Contains(new LatenessSource("a", 0), sources);
        Assert.Contains(new LatenessSource("b", 0), sources);
        Assert.Equal(sources, info.Sources(join, 2)); // both key columns share the union
    }

    [Fact]
    public void InnerJoin_MonotoneNonKeyColumn_LosesMonotonicity()
    {
        // a.ts is monotone but is NOT the join key — after the join it pairs
        // with arbitrarily-old b rows, so it can no longer be bounded.
        var a = Scan("a", new[] { "k", "ts" }, 0, 1);
        var b = Scan("b", new[] { "k", "y" }, 0);
        var join = new JoinPlan(
            a, b, JoinType.Inner,
            new[] { new JoinEquality(0, 0, Bigint) },
            Residual: null,
            Schema: a.Schema.Concat(b.Schema));
        var info = MonotonicityAnalyzer.Analyze(join);

        Assert.True(info.IsMonotone(join, 0));  // a.k (key)
        Assert.False(info.IsMonotone(join, 1)); // a.ts (monotone but non-key) → lost
    }

    [Fact]
    public void LeftOuterJoin_PreservedKeyMonotone_NullablledKeyNot()
    {
        var a = Scan("a", new[] { "k", "x" }, 0);
        var b = Scan("b", new[] { "k", "y" }, 0);
        var join = new JoinPlan(
            a, b, JoinType.LeftOuter,
            new[] { new JoinEquality(0, 0, Bigint) },
            Residual: null,
            Schema: a.Schema.Concat(b.Schema));
        var info = MonotonicityAnalyzer.Analyze(join);

        Assert.True(info.IsMonotone(join, 0));  // left key carries the real value
        Assert.False(info.IsMonotone(join, 2)); // right key is NULL for unmatched rows
        var sources = info.Sources(join, 0)!;
        Assert.Single(sources); // preserved side only — not a min/union
        Assert.Contains(new LatenessSource("a", 0), sources);
    }

    [Fact]
    public void Aggregate_GroupByMonotoneColumn_StaysMonotone()
    {
        var scan = Scan("t", new[] { "ts", "v" }, 0);
        var agg = new AggregatePlan(
            scan,
            new ResolvedExpression[] { Col(0) },
            new[] { new AggregateCall(AggregateKind.CountStar, null, Bigint) },
            Sch("ts", "cnt"));
        var info = MonotonicityAnalyzer.Analyze(agg);

        Assert.True(info.IsMonotone(agg, 0));  // group key ts
        Assert.False(info.IsMonotone(agg, 1)); // COUNT(*) result
    }

    [Fact]
    public void Union_MonotoneInAllBranches_OrNot()
    {
        var a = Scan("a", new[] { "ts", "v" }, 0);
        var b = Scan("b", new[] { "ts", "v" }, 0);
        var union = new UnionAllPlan(new LogicalPlan[] { a, b }, Sch("ts", "v"));
        var info = MonotonicityAnalyzer.Analyze(union);

        Assert.True(info.IsMonotone(union, 0));
        Assert.Equal(2, info.Sources(union, 0)!.Count); // union {a, b}

        // If one branch's column is not monotone, the union column is not either.
        var bNoLateness = Scan("b", new[] { "ts", "v" });
        var union2 = new UnionAllPlan(new LogicalPlan[] { a, bNoLateness }, Sch("ts", "v"));
        var info2 = MonotonicityAnalyzer.Analyze(union2);
        Assert.False(info2.IsMonotone(union2, 0));
    }

    [Fact]
    public void Distinct_PassesMonotonicityThrough()
    {
        var scan = Scan("t", new[] { "ts", "v" }, 0);
        var distinct = new DistinctPlan(scan);
        Assert.True(MonotonicityAnalyzer.Analyze(distinct).IsMonotone(distinct, 0));
    }

    [Fact]
    public void FullPropagation_FrontierSurvivesFilterAndProjectToGroupBy()
    {
        // The headline full-reach property: ts declared late on the source still
        // drives the GROUP BY's GC after an intervening WHERE and projection.
        var scan = Scan("events", new[] { "ts", "v" }, 0);
        var filter = new FilterPlan(scan, Col(1));
        var project = new ProjectPlan(
            filter,
            new[] { new ProjectionItem(Col(0), "ts"), new ProjectionItem(Col(1), "v") },
            Sch("ts", "v"));
        var agg = new AggregatePlan(
            project,
            new ResolvedExpression[] { Col(0) },
            new[] { new AggregateCall(AggregateKind.CountStar, null, Bigint) },
            Sch("ts", "cnt"));
        var info = MonotonicityAnalyzer.Analyze(agg);

        Assert.True(info.IsMonotone(agg, 0));
        Assert.Contains(new LatenessSource("events", 0), info.Sources(agg, 0)!);
    }
}
