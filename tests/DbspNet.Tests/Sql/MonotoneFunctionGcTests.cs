// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Phase-4 LATENESS GC through monotone scalar functions: a GROUP BY over a
/// derived <c>date_trunc(ts)</c> or <c>ts + interval</c> column keeps bounded
/// state because the analyzer propagates the frontier (and, for date_trunc,
/// transforms it into the truncated-key space). The transform is the soundness
/// crux — using the raw frontier would collect a window a future in-window row
/// still maps into.
/// </summary>
public class MonotoneFunctionGcTests
{
    private const long Day = 86_400_000_000L;
    private const long Hour = 3_600_000_000L;
    private const long TwoDays = 2 * Day;

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

    private static int RetainedGroups(CompiledQuery q) =>
        q.Circuit.Operators
            .OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>()
            .Single()
            .RetainedGroupCount;

    [Fact]
    public void GroupByDateTruncDay_BoundsStateWithTransformedFrontier()
    {
        // ts at noon each day; LATENESS = 2 days. Group key = date_trunc('day', ts)
        // = midnight. The frontier (maxSeen − 2d = noon of day N−2) must be
        // truncated to midnight of day N−2 before thresholding the midnight keys,
        // or day N−2 would be wrongly collected.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"],
            "SELECT day, COUNT(*) FROM (SELECT date_trunc('day', ts) AS day FROM events) sub GROUP BY day");

        for (long i = 0; i <= 20; i++)
        {
            q.Table("events").Insert(new Timestamp(i * Day + 12 * Hour), 1);
            q.Step();
        }

        // Retained days = {18, 19, 20} (the trailing 2-day window + the current
        // day). Raw-frontier GC would (wrongly) keep only {19, 20}.
        Assert.Equal(3, RetainedGroups(q));
        Assert.Equal(1, q.WeightOf(new Timestamp(20 * Day), 1L).Value); // midnight day 20, count 1
    }

    [Fact]
    public void GroupByDateTrunc_InWindowRowUpdatesRetainedGroup_NotResurrected()
    {
        // Soundness witness: after the window has advanced, a fresh row that still
        // falls in a retained truncated-day group must UPDATE that group (count→2),
        // proving it was kept in the trace — not collected and recreated.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"],
            "SELECT day, COUNT(*) FROM (SELECT date_trunc('day', ts) AS day FROM events) sub GROUP BY day");

        for (long i = 0; i <= 20; i++)
        {
            q.Table("events").Insert(new Timestamp(i * Day + 12 * Hour), 1);
            q.Step();
        }

        // 1pm on day 18 ≥ the frontier (noon day 18), so it is admitted; its group
        // is midnight day 18, which is retained.
        q.Table("events").Insert(new Timestamp(18 * Day + 13 * Hour), 1);
        q.Step();

        Assert.Equal(3, RetainedGroups(q)); // still bounded, no resurrected group
        // Day-18 group now has count 2 (a buggy raw-frontier GC would have dropped
        // it and recreated it at count 1).
        Assert.Equal(1, q.WeightOf(new Timestamp(18 * Day), 2L).Value);
    }

    [Fact]
    public void GroupByTsPlusConstant_BoundsStateConservatively()
    {
        // shifted = ts + 5 is a forward shift: monotone, and the raw frontier is a
        // sound (conservative) threshold — identity transform, no runtime change.
        var q = Compile(
            ["CREATE TABLE events (ts BIGINT NOT NULL LATENESS 10, v INT NOT NULL)"],
            "SELECT shifted, COUNT(*) FROM (SELECT ts + 5 AS shifted FROM events) sub GROUP BY shifted");

        for (long t = 0; t <= 200; t++)
        {
            q.Table("events").Insert(t, 1);
            q.Step();
        }

        // frontier = 190; GC drops shifted < 190 ⇒ ts < 185 ⇒ retained ts ∈ [185,200]
        // = 16 groups (the +5 shift makes the raw frontier loose by the constant).
        Assert.Equal(16, RetainedGroups(q));
        Assert.Equal(1, q.WeightOf(205L, 1L).Value); // shifted = 200 + 5
    }

    [Fact]
    public void GroupByTsPlusInterval_BoundsState()
    {
        // ts + INTERVAL '1' DAY — a non-negative interval shift; identity transform.
        var q = Compile(
            ["CREATE TABLE events (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"],
            "SELECT shifted, COUNT(*) FROM (SELECT ts + INTERVAL '1' DAY AS shifted FROM events) sub GROUP BY shifted");

        for (long i = 0; i <= 20; i++)
        {
            q.Table("events").Insert(new Timestamp(i * Day), 1); // midnight day i
            q.Step();
        }

        // frontier (raw) = maxSeen − 2d = midnight day 18. shifted = ts + 1d.
        // GC drops shifted < midnight day 18 ⇒ ts + 1d < day18 ⇒ ts < day17 ⇒
        // retained ts ∈ {17,18,19,20} = 4 groups (conservative by the 1-day shift).
        Assert.Equal(4, RetainedGroups(q));
        Assert.Equal(1, q.WeightOf(new Timestamp(21 * Day), 1L).Value); // 20d + 1d shift
    }
}
