// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Oracle check for phase-4 monotone-function GC: a GROUP BY over a derived
/// <c>date_trunc(ts)</c> column, run incrementally with the transformed-frontier
/// trace GC, must produce exactly the batch answer. A monotone stream admits
/// every row (nothing is late), so any divergence would be the GC dropping state
/// that still affects output — i.e. an unsound frontier transform.
/// </summary>
public class MonotoneFunctionOracleTests
{
    private const long Day = 86_400_000_000L;
    private const long Hour = 3_600_000_000L;

    [Fact]
    public void DateTruncGroupBy_IncrementalGcEqualsBatch()
    {
        string[] ddl = ["CREATE TABLE a (ts TIMESTAMP NOT NULL LATENESS 172800000000, v INT NOT NULL)"]; // 2 days
        const string sql =
            "SELECT day, COUNT(*) AS c FROM (SELECT date_trunc('day', ts) AS day, v FROM a) sub GROUP BY day";

        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        var compiled = PlanToCircuit.Compile(plan);

        // Strictly increasing timestamps (two rows per day) over 11 days: the
        // frontier advances and GC reclaims old days, but every row is admitted.
        var events = new List<InputEvent>();
        var ticks = new List<IReadOnlyList<InputEvent>>();
        for (long d = 0; d <= 10; d++)
        {
            foreach (var h in new[] { 6L, 18L })
            {
                var e = new InputEvent("a", new object?[] { new Timestamp(d * Day + h * Hour), (int)h }, 1L);
                events.Add(e);
                ticks.Add(new[] { e });
            }
        }

        var accumulated = IncrementalOracle.RunAndAccumulate(compiled, ticks);

        var ctx = new BatchEvalContext(
            new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
            {
                ["a"] = IncrementalOracle.NetTable(events, "a"),
            },
            new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
        var batch = BatchPlanEvaluator.Evaluate(plan, ctx);

        Assert.Equal(batch, accumulated);
    }
}
