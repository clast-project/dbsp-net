// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Reflection;
using System.Text.Json;
using DbspNet.Core.Circuit;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Optimizer;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks;

/// <summary>
/// <c>statecensus &lt;program.json&gt;</c> — for an ivm-bench program spec (the JSON
/// <c>dbt_to_program.py</c> emits), list which stateful operator kinds each view uses on
/// the structural program path, and which state-representation variants
/// (docs/design-state-representation.md A / B / B2 / G) each would be eligible for.
/// Compile-only: no data is loaded.
/// </summary>
internal static class StateCensus
{
    public static int Run(string[] args)
    {
        var path = args.Length > 1 ? args[1] : "program.json";
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var statements = doc.RootElement.GetProperty("program").EnumerateArray().Select(e => e.GetString()!).ToList();
        var outputs = doc.RootElement.GetProperty("outputs").EnumerateArray().Select(e => e.GetString()!)
            .ToHashSet(StringComparer.Ordinal);

        var resolved = SqlProgram.Resolve(statements, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);
        var totals = new Dictionary<string, int>(StringComparer.Ordinal);
        var viewsBy = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        void Hit(string kind, string view)
        {
            totals[kind] = totals.GetValueOrDefault(kind) + 1;
            if (!viewsBy.TryGetValue(kind, out var s)) viewsBy[kind] = s = new SortedSet<string>(StringComparer.Ordinal);
            s.Add(view);
        }

        Console.WriteLine("## Per-view plan census (optimized plans)");
        foreach (var v in resolved.Views)
        {
            var plan = PlanOptimizer.Optimize(v.Query);
            var nodes = new List<LogicalPlan>();
            Walk(plan, nodes);
            var lines = new List<string>();
            foreach (var n in nodes)
            {
                switch (n)
                {
                    case AggregatePlan a:
                        {
                            var kinds = string.Join(",", a.Aggregates.Select(Describe));
                            var hasMinMax = a.Aggregates.Any(c => c.Kind is AggregateKind.Min or AggregateKind.Max);
                            var hasCd = a.Aggregates.Any(c => c.Kind == AggregateKind.CountDistinct);
                            var linearStrict = a.Aggregates.All(IsLinearStrict);
                            var linearLoose = a.Aggregates.All(IsLinearLoose);
                            var tag = a.GroupKeys.Count == 0 ? "global" : $"{a.GroupKeys.Count}keys";
                            lines.Add($"  AGG[{tag}] {kinds}" +
                                      (hasMinMax ? " [B2:minmax]" : "") + (hasCd ? " [B:cd]" : "") +
                                      (linearStrict ? " [G:eligible]" : linearLoose ? " [G:if-nullable-SUM]" : ""));
                            Hit("AggregatePlan", v.ViewName);
                            if (hasMinMax) Hit("Agg with MIN/MAX (B2)", v.ViewName);
                            if (hasCd) Hit("Agg with COUNT(DISTINCT) (B)", v.ViewName);
                            if (linearStrict) Hit("Agg all-linear, typed G rules (G)", v.ViewName);
                            if (linearLoose) Hit("Agg all-linear incl nullable SUM (G')", v.ViewName);
                            break;
                        }

                    case PartitionedTopKPlan t:
                        lines.Add($"  PTOPK {t.Function} parts={t.PartitionKeys.Count} order={t.SortKeys.Count} limit={t.Limit}");
                        Hit(t.PartitionKeys.Count > 0 ? "PartitionedTopK (A)" : "PartitionedTopK global", v.ViewName);
                        break;
                    case PartitionedRankPlan r:
                        lines.Add($"  RANK {r.Function} parts={r.PartitionKeys.Count} order={r.SortKeys.Count}");
                        Hit("PartitionedRank (rank-in-output)", v.ViewName);
                        break;
                    case WindowAggregatePlan w:
                        lines.Add($"  WINAGG parts={w.PartitionKeys.Count} order={(w.OrderKey is null ? 0 : 1)} {string.Join(",", w.Aggregates.Select(Describe))}");
                        Hit("WindowAggregate", v.ViewName);
                        break;
                    case WindowOffsetPlan o:
                        lines.Add($"  WINOFF parts={o.PartitionKeys.Count} {string.Join(",", o.Functions.Select(f => f.Kind.ToString()))}");
                        Hit("WindowOffset", v.ViewName);
                        break;
                    case TopKPlan:
                        lines.Add("  TOPK");
                        Hit("TopK", v.ViewName);
                        break;
                    case DistinctPlan:
                        lines.Add("  DISTINCT");
                        Hit("Distinct", v.ViewName);
                        break;
                    case JoinPlan j:
                        lines.Add($"  JOIN {j.GetType().GetProperty("Kind")?.GetValue(j)}");
                        Hit("Join", v.ViewName);
                        break;
                }
            }

            if (lines.Count > 0)
            {
                Console.WriteLine($"{v.ViewName}:");
                foreach (var l in lines) Console.WriteLine(l);
            }
        }

        Console.WriteLine();
        Console.WriteLine("## Plan-node totals (nodes / distinct views)");
        foreach (var (k, n) in totals.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            Console.WriteLine($"  {k}: {n} nodes in {viewsBy[k].Count} views [{string.Join(", ", viewsBy[k])}]");
        }

        // Compile the program as the DbspNet server does (default options, W=1) and count
        // the operator types actually instantiated, per view label.
        var program = SqlProgram.Compile(statements, outputs, numericStringCoercion: true, nullCollation: NullCollation.Low);
        var circuit = program.Circuit;
        var ops = circuit.Operators;
        var labels = (List<string?>)typeof(RootCircuit)
            .GetField("_operatorLabels", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(circuit)!;
        var byType = new Dictionary<string, (int Count, SortedSet<string> Views)>(StringComparer.Ordinal);
        for (var i = 0; i < ops.Count; i++)
        {
            var name = ops[i].GetType().Name;
            var tick = name.IndexOf('`');
            if (tick >= 0) name = name[..tick];
            if (!byType.TryGetValue(name, out var e)) e = (0, new SortedSet<string>(StringComparer.Ordinal));
            e.Views.Add(i < labels.Count ? labels[i] ?? "(none)" : "(none)");
            byType[name] = (e.Count + 1, e.Views);
        }

        Console.WriteLine();
        Console.WriteLine($"## Compiled operator types (structural program path, {ops.Count} operators)");
        foreach (var (k, (n, views)) in byType.OrderByDescending(kv => kv.Value.Count))
        {
            var vs = views.Count <= 12 ? string.Join(", ", views) : $"{views.Count} views";
            Console.WriteLine($"  {k}: {n}  [{vs}]");
        }

        return 0;
    }

    private static string Describe(AggregateCall c)
    {
        var arg = c.Argument is null ? "*" : (c.Argument.Type.Nullable ? "null" : "nn") + ":" + c.Argument.Type.GetType().Name.Replace("Sql", "").Replace("Type", "");
        return $"{c.Kind}({arg})";
    }

    // The typed G operator's rules (TypedPlanCompiler.LinearAggregate.cs).
    private static bool IsLinearStrict(AggregateCall c) => c.Kind switch
    {
        AggregateKind.CountStar or AggregateKind.Count or AggregateKind.Avg or AggregateKind.CountDistinct => true,
        AggregateKind.Sum => c.Argument is { Type.Nullable: false },
        _ => false,
    };

    private static bool IsLinearLoose(AggregateCall c) =>
        c.Kind is AggregateKind.CountStar or AggregateKind.Count or AggregateKind.Avg
            or AggregateKind.CountDistinct or AggregateKind.Sum;

    private static void Walk(LogicalPlan plan, List<LogicalPlan> acc) =>
        WalkObj(plan, acc, new HashSet<object>(ReferenceEqualityComparer.Instance));

    // Deep walk of every DbspNet.Sql object reachable from the plan (CTE refs, subquery
    // plans inside expressions, ...), collecting each LogicalPlan once.
    private static void WalkObj(object? o, List<LogicalPlan> acc, HashSet<object> seen)
    {
        if (o is null || o is string || o.GetType().IsPrimitive || o is Type) return;
        if (!seen.Add(o)) return;
        if (o is LogicalPlan lp) acc.Add(lp);
        if (o is IEnumerable items && o.GetType().Namespace?.StartsWith("DbspNet.Sql", StringComparison.Ordinal) != true)
        {
            foreach (var item in items) WalkObj(item, acc, seen);
            return;
        }

        if (o.GetType().Namespace?.StartsWith("DbspNet.Sql", StringComparison.Ordinal) != true) return;
        foreach (var p in o.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (p.GetIndexParameters().Length > 0) continue;
            if (p.PropertyType.IsPrimitive || p.PropertyType == typeof(string)) continue;
            if (typeof(Schema).IsAssignableFrom(p.PropertyType) || typeof(SqlType).IsAssignableFrom(p.PropertyType)) continue;
            object? v;
            try { v = p.GetValue(o); } catch { continue; }
            WalkObj(v, acc, seen);
        }
    }
}
