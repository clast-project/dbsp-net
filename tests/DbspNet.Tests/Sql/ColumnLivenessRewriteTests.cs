// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Program-level dead-column elimination (docs/design-column-liveness.md,
/// <see cref="CompileOptions.EliminateDeadColumns"/>): a view column read by no
/// output and no live downstream view is pruned, and a window/offset operator all
/// of whose produced columns are dead is eliminated. The rewrite is arity- and
/// result-preserving: compiling with the flag on must give byte-identical output on
/// every live view.
/// </summary>
public class ColumnLivenessRewriteTests
{
    private static StructuralRow Row(params object?[] values) => new(values);

    private static readonly HashSet<string> None = new(StringComparer.Ordinal);

    private static CompiledProgram Compile(ResolvedProgram r, bool prune) =>
        PlanToCircuit.CompileProgram(
            r.Tables, r.Views, null,
            new CompileOptions { EliminateDeadColumns = prune });

    // Count nodes of a given plan type anywhere in a plan tree (through CTE bodies).
    private static int Count<T>(LogicalPlan p) where T : LogicalPlan
    {
        var n = p is T ? 1 : 0;
        switch (p)
        {
            case CteScanPlan c: n += Count<T>(c.Cte.Plan); break;
            case FilterPlan f: n += Count<T>(f.Input); break;
            case ProjectPlan pr: n += Count<T>(pr.Input); break;
            case JoinPlan j: n += Count<T>(j.Left) + Count<T>(j.Right); break;
            case AggregatePlan a: n += Count<T>(a.Input); break;
            case WindowAggregatePlan wa: n += Count<T>(wa.Input); break;
            case WindowOffsetPlan wo: n += Count<T>(wo.Input); break;
            case DistinctPlan d: n += Count<T>(d.Input); break;
            case UnionAllPlan u: n += u.Branches.Sum(Count<T>); break;
        }

        return n;
    }

    [Fact]
    public void ProducerDeadWindow_IsEliminated_OutputByteIdentical()
    {
        string[] program =
        [
            "CREATE TABLE src (sym INT NOT NULL, d INT NOT NULL, v INT NOT NULL)",
            // window column rmax is produced here...
            "CREATE VIEW w AS SELECT sym, d, v, " +
            "  MAX(v) OVER (PARTITION BY sym ORDER BY d) AS rmax FROM src",
            // ...read by nobody live (this leaf is not an output and has no consumer)...
            "CREATE VIEW deadleaf AS SELECT rmax FROM w",
            // ...only sym,d,v are live.
            "CREATE VIEW live AS SELECT sym, d, v FROM w",
        ];
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "live" };
        var resolved = SqlProgram.Resolve(program, outputs);

        // The rewrite eliminates w's window aggregate (rmax is dead).
        var wView = resolved.Views.First(v => v.ViewName == "w");
        var live = PlanColumnLiveness.ComputeProgramLiveColumns(resolved.Views);
        Assert.Equal(1, Count<WindowAggregatePlan>(wView.Query));
        var pruned = PlanColumnLiveness.PruneDeadColumns(wView.Query, live["w"]);
        Assert.Equal(0, Count<WindowAggregatePlan>(pruned)); // gone

        // And the live output is byte-identical with vs without the flag.
        var baseP = Compile(resolved, prune: false);
        var pruneP = Compile(resolved, prune: true);
        foreach (var (a, b, c) in new[] { (1, 1, 5), (1, 2, 9), (1, 3, 3), (2, 1, 7), (2, 2, 4) })
        {
            baseP.Table("src").Insert(a, b, c);
            pruneP.Table("src").Insert(a, b, c);
        }

        baseP.Step();
        pruneP.Step();

        Assert.True(
            pruneP.Outputs["live"].CurrentView.Equals(baseP.Outputs["live"].CurrentView),
            $"live\n  base={baseP.Outputs["live"].CurrentView}\n  prune={pruneP.Outputs["live"].CurrentView}");
        Assert.Equal(5, baseP.Outputs["live"].CurrentView.Count);
    }

    [Fact]
    public void DeadGroupKey_GroupingPreserved()
    {
        // k is a GROUP BY key surfaced to output but read by no live consumer. The
        // pass must NOT change the grouping (dropping k would merge (g=1,k=1) and
        // (g=1,k=2)); the SUM(x) per (g,k) must stay correct.
        string[] program =
        [
            "CREATE TABLE t (g INT NOT NULL, k INT NOT NULL, x INT NOT NULL)",
            "CREATE VIEW agg AS SELECT g, k, SUM(x) AS sx FROM t GROUP BY g, k",
            "CREATE VIEW consumer AS SELECT g, sx FROM agg", // reads g, sx — not k
        ];
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "consumer" };
        var resolved = SqlProgram.Resolve(program, outputs);

        var baseP = Compile(resolved, prune: false);
        var pruneP = Compile(resolved, prune: true);
        foreach (var (g, k, x) in new[] { (1, 1, 10), (1, 2, 20), (1, 1, 5), (2, 1, 7) })
        {
            baseP.Table("t").Insert(g, k, x);
            pruneP.Table("t").Insert(g, k, x);
        }

        baseP.Step();
        pruneP.Step();

        Assert.True(
            pruneP.Outputs["consumer"].CurrentView.Equals(baseP.Outputs["consumer"].CurrentView),
            $"consumer\n  base={baseP.Outputs["consumer"].CurrentView}\n  prune={pruneP.Outputs["consumer"].CurrentView}");
        // Three groups: (g=1,k=1)->15, (g=1,k=2)->20, (g=2,k=1)->7 => consumer rows
        // (g=1,15),(g=1,20),(g=2,7). If grouping had collapsed on a dropped k, g=1
        // would wrongly sum to 35.
        Assert.Equal(1, pruneP.Outputs["consumer"].CurrentView.WeightOf(Row(1, 15L)).Value);
        Assert.Equal(1, pruneP.Outputs["consumer"].CurrentView.WeightOf(Row(1, 20L)).Value);
        Assert.Equal(1, pruneP.Outputs["consumer"].CurrentView.WeightOf(Row(2, 7L)).Value);
    }

    [Fact]
    public void DeadDistinctKey_MultiplicityPreserved()
    {
        // b is part of a SELECT DISTINCT key but read by no live consumer. Dropping
        // it before the DISTINCT would over-dedup and change the downstream
        // multiplicity — the pass must keep it (DISTINCT forces full liveness).
        string[] program =
        [
            "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
            "CREATE VIEW d AS SELECT DISTINCT a, b FROM t",
            "CREATE VIEW consumer AS SELECT a FROM d", // reads a — not b
        ];
        var outputs = new HashSet<string>(StringComparer.Ordinal) { "consumer" };
        var resolved = SqlProgram.Resolve(program, outputs);

        var baseP = Compile(resolved, prune: false);
        var pruneP = Compile(resolved, prune: true);
        // (1,10),(1,20),(1,10): DISTINCT => {(1,10),(1,20)}; consumer SELECT a => a=1
        // with weight 2. Over-dedup on a alone would give weight 1.
        foreach (var (a, b) in new[] { (1, 10), (1, 20), (1, 10) })
        {
            baseP.Table("t").Insert(a, b);
            pruneP.Table("t").Insert(a, b);
        }

        baseP.Step();
        pruneP.Step();

        Assert.True(
            pruneP.Outputs["consumer"].CurrentView.Equals(baseP.Outputs["consumer"].CurrentView),
            $"consumer\n  base={baseP.Outputs["consumer"].CurrentView}\n  prune={pruneP.Outputs["consumer"].CurrentView}");
        Assert.Equal(2, pruneP.Outputs["consumer"].CurrentView.WeightOf(Row(1)).Value);
    }
}
