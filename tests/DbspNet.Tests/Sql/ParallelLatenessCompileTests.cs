// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Which parallel compiles take LATENESS and temporal filters (docs/design-parallel-frontier.md §2 Q5).
/// </summary>
public class ParallelLatenessCompileTests
{
    private static LogicalPlan Plan(string ddl, string sql)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(ddl));
        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
    }

    [Fact]
    public void StructuralSingleQuery_TakesLateness_AndDropsAgainstTheGlobalMax()
    {
        var plan = Plan(
            "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "SELECT ts, SUM(v) AS s FROM e GROUP BY ts");
        Assert.True(PlanToCircuit.TryCompileParallel(plan, 4, out var q));
        using (q)
        {
            q!.Table("e").Insert(100L, 1);
            q.Step();
            q.Table("e").Insert(10L, 1);
            q.Table("e").Insert(96L, 1);
            q.Step();
            Assert.Equal(1, q.Current.Count); // only 96: 10 is late on whichever shard it lands
        }
    }

    [Fact]
    public void TypedParallel_RefusesLateness_RatherThanIgnoringIt()
    {
        var plan = Plan(
            "CREATE TABLE e (ts BIGINT NOT NULL LATENESS 5, v INT NOT NULL)",
            "SELECT ts, SUM(v) AS s FROM e GROUP BY ts");
        Assert.False(TypedPlanCompiler.TryCompileParallel(plan, 2, out var q));
        Assert.Null(q);
    }

    [Fact]
    public void BothParallelPaths_TakeATemporalFilter()
    {
        const string Ddl = "CREATE TABLE a (ts TIMESTAMP NOT NULL, v INT NOT NULL)";
        const string Query = "SELECT ts, v FROM a WHERE ts > NOW() - INTERVAL '3' SECOND";
        Assert.True(PlanToCircuit.TryCompileParallel(Plan(Ddl, Query), 2, out var q));
        q!.Dispose();

        // The program path too, since docs/design-program-temporal.md §2 Q3.
        string[] program = [Ddl, "CREATE VIEW recent AS " + Query];
        var views = new HashSet<string>(["recent"], StringComparer.Ordinal);
        Assert.Empty(SqlProgram.ParallelBlockers(program, views));
        Assert.True(SqlProgram.TryCompileParallel(program, views, 2, out var p));
        p!.Dispose();
    }
}
