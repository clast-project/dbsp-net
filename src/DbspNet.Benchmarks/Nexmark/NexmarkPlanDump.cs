// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Reflection;
using System.Text;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Benchmarks.Nexmark;

/// <summary>
/// Diagnostic: compile a Nexmark query to its optimized <see cref="LogicalPlan"/>
/// and dump the operator tree, so structural redundancy is visible. Walks child
/// plans via reflection (each plan record exposes its inputs as named
/// <see cref="LogicalPlan"/>-typed or <c>IEnumerable&lt;LogicalPlan&gt;</c>
/// properties). Reference-identical subtrees (the plan→circuit compiler dedups by
/// reference, see LogicalPlan.cs) are tagged <c>[shared #n]</c> the second time
/// they appear; a subtree that recurs by VALUE but not by reference prints in full
/// twice — that is a duplicated computation the circuit will materialize twice.
/// </summary>
internal static class NexmarkPlanDump
{
    public static void Run(StringBuilder o, string queryId)
    {
        var query = NexmarkQueries.All.FirstOrDefault(q =>
            string.Equals(q.Id, queryId, StringComparison.OrdinalIgnoreCase));
        if (query is null)
        {
            o.AppendLine($"unknown query '{queryId}'. Known: {string.Join(", ", NexmarkQueries.All.Select(q => q.Id))}");
            Console.WriteLine(o.ToString());
            return;
        }

        var plan = BuildPlan(NexmarkQueries.Ddl, query.Sql);

        // Compile it to confirm shared subplans compile once: MemoHits counts how
        // many reference-shared subplans the compiler served from cache.
        DbspNet.Sql.Compiler.PlanToCircuit.MemoHits = 0;
        DbspNet.Sql.Compiler.PlanToCircuit.MemoMisses = 0;
        DbspNet.Sql.Compiler.PlanToCircuit.Compile(plan);
        var hits = DbspNet.Sql.Compiler.PlanToCircuit.MemoHits;
        var misses = DbspNet.Sql.Compiler.PlanToCircuit.MemoMisses;

        var kinds = new Dictionary<string, int>(StringComparer.Ordinal);
        var seen = new Dictionary<LogicalPlan, int>(ReferenceEqualityComparer.Instance);
        var sb = new StringBuilder();
        Dump(sb, plan, 0, kinds, seen);

        o.AppendLine($"# Nexmark {query.Id} — optimized plan tree");
        o.AppendLine();
        o.AppendLine($"_{query.Description}_");
        o.AppendLine();
        o.AppendLine($"**Compile memo:** {hits} shared-subplan hits, {misses} misses (hits > 0 ⇒ CSE sharing reached the compiler).");
        o.AppendLine();
        o.AppendLine("## Operator counts (by plan-node kind)");
        o.AppendLine();
        o.AppendLine("| kind | count |");
        o.AppendLine("|:--|--:|");
        foreach (var (k, v) in kinds.OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.Ordinal))
        {
            o.AppendLine($"| {k} | {v} |");
        }

        o.AppendLine();
        o.AppendLine("## Tree (`[shared #n]` = reference-identical to an earlier node)");
        o.AppendLine();
        o.AppendLine("```");
        o.Append(sb);
        o.AppendLine("```");
        Console.WriteLine(o.ToString());
    }

    private static LogicalPlan BuildPlan(string[] ddl, string sql)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        return PlanOptimizer.Optimize(plan);
    }

    private static void Dump(
        StringBuilder sb, LogicalPlan plan, int depth,
        Dictionary<string, int> kinds, Dictionary<LogicalPlan, int> seen)
    {
        var indent = new string(' ', depth * 2);
        var kind = plan.GetType().Name;

        if (seen.TryGetValue(plan, out var firstId))
        {
            sb.AppendLine($"{indent}{kind} [shared #{firstId}]");
            return; // do not re-count or re-descend a reference-shared subtree
        }

        var id = seen.Count;
        seen[plan] = id;
        kinds[kind] = kinds.GetValueOrDefault(kind) + 1;
        sb.AppendLine($"{indent}{kind}{Detail(plan)}  (#{id})");

        foreach (var child in Children(plan))
        {
            Dump(sb, child, depth + 1, kinds, seen);
        }
    }

    // A short discriminator so aggregates/joins/scans are legible in the tree.
    private static string Detail(LogicalPlan plan) => plan switch
    {
        ScanPlan s => ScanName(s),
        AggregatePlan a => $" (groupKeys={a.GroupKeys.Count}, aggs={a.Aggregates.Count})",
        JoinPlan j => $" ({j.JoinType}, equiKeys={j.EquiKeys.Count}{(j.Residual is null ? "" : ", +residual")})",
        UnionAllPlan u => $" (branches={u.Branches.Count})",
        _ => "",
    };

    private static string ScanName(ScanPlan s)
    {
        // ScanPlan's table name field name varies; surface whatever string prop it has.
        var prop = typeof(ScanPlan).GetProperties()
            .FirstOrDefault(p => p.PropertyType == typeof(string));
        var name = prop?.GetValue(s) as string;
        return name is null ? "" : $" ({name})";
    }

    // Enumerate child LogicalPlans via reflection: every property typed as
    // LogicalPlan, or IEnumerable<LogicalPlan>, in declaration order.
    private static IEnumerable<LogicalPlan> Children(LogicalPlan plan)
    {
        foreach (var p in plan.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (typeof(LogicalPlan).IsAssignableFrom(p.PropertyType))
            {
                if (p.GetValue(plan) is LogicalPlan child)
                {
                    yield return child;
                }
            }
            else if (p.PropertyType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(p.PropertyType))
            {
                if (p.GetValue(plan) is IEnumerable seq)
                {
                    foreach (var item in seq)
                    {
                        if (item is LogicalPlan child)
                        {
                            yield return child;
                        }
                    }
                }
            }
        }
    }
}
