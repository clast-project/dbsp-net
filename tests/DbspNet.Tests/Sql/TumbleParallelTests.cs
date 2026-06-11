// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Guards that event-time tumbling-window queries take the typed → data-parallel
/// (W&gt;1) path. This rides on two typed-compiler capabilities the windowing arc
/// added: a typed lowering for <c>tumble_start</c> (the window-start floor) and
/// typed temporal±INTERVAL arithmetic (for <c>TUMBLE_END = tumble_start + size</c>,
/// including the post-aggregate <c>CAST(string AS INTERVAL)</c> the resolver leaves
/// unfolded there). Without either, the whole query falls back to the structural,
/// single-circuit compile (Nexmark q8/q12 regress to single-only). A failure here
/// means a windowed query silently lost its parallel form.
/// </summary>
public class TumbleParallelTests
{
    private static readonly string[] Ddl =
    {
        @"CREATE TABLE bid (auction BIGINT NOT NULL, bidder BIGINT NOT NULL, price BIGINT NOT NULL,
            channel VARCHAR NOT NULL, url VARCHAR NOT NULL, date_time TIMESTAMP NOT NULL, extra VARCHAR NOT NULL)",
    };

    private static bool CompilesParallel(string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = PlanOptimizer.Optimize(((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query);
        var ok = TypedPlanCompiler.TryCompileParallel(plan, 4, out var q);
        q?.Dispose();
        return ok;
    }

    [Fact]
    public void Q12Shape_PerBidderWindowCounts_TakesParallelPath() =>
        Assert.True(CompilesParallel(
            @"SELECT bidder, COUNT(*) AS c,
                     TUMBLE_START(date_time, INTERVAL '10' SECOND) AS ws,
                     TUMBLE_END(date_time, INTERVAL '10' SECOND) AS we
              FROM bid GROUP BY bidder, TUMBLE(date_time, INTERVAL '10' SECOND)"));

    [Fact]
    public void TumbleStartGroupKey_TakesParallelPath() =>
        Assert.True(CompilesParallel(
            @"SELECT TUMBLE_START(date_time, INTERVAL '10' SECOND) AS ws, COUNT(*) AS c
              FROM bid GROUP BY TUMBLE(date_time, INTERVAL '10' SECOND)"));

    [Fact]
    public void TimestampPlusInterval_Projection_TakesParallelPath() =>
        Assert.True(CompilesParallel("SELECT date_time + INTERVAL '10' SECOND AS t FROM bid"));

    [Fact]
    public void TimestampMinusInterval_Filter_TakesParallelPath() =>
        Assert.True(CompilesParallel(
            "SELECT bidder FROM bid WHERE date_time > date_time - INTERVAL '5' SECOND"));
}
