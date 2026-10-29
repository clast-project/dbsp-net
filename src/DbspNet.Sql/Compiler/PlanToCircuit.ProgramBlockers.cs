// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

public static partial class PlanToCircuit
{
    /// <summary>
    /// Program-level <see cref="ParallelBlockers(LogicalPlan)"/>: for every view reachable
    /// from an output, the constructs that keep <see cref="TryCompileProgramParallel"/> from
    /// sharding it. Checks the same plans that compile does (dead-view elimination, then
    /// dead-column pruning when enabled, then the optimizer), so an empty result means the
    /// parallel compile is not refused for shardability. Only views with at least one
    /// blocker appear, in dependency order.
    /// </summary>
    internal static IReadOnlyList<(string View, IReadOnlyList<string> Blockers)> ProgramParallelBlockers(
        IReadOnlyList<ProgramView> views, CompileOptions? options = null)
    {
        options ??= CompileOptions.Default;
        var reachable = new HashSet<string>(StringComparer.Ordinal);
        for (var i = views.Count - 1; i >= 0; i--)
        {
            var v = views[i];
            if (v.IsOutput)
            {
                reachable.Add(v.ViewName);
            }

            if (reachable.Contains(v.ViewName))
            {
                foreach (var referenced in CollectScans(v.Query).Scans.Keys)
                {
                    reachable.Add(referenced);
                }
            }
        }

        var liveColumns = options.EliminateDeadColumns
            ? DbspNet.Sql.Optimizer.PlanColumnLiveness.ComputeProgramLiveColumns(views)
            : null;

        var result = new List<(string, IReadOnlyList<string>)>();
        foreach (var v in views)
        {
            if (!reachable.Contains(v.ViewName))
            {
                continue;
            }

            var query = v.Query;
            if (liveColumns is not null && liveColumns.TryGetValue(v.ViewName, out var liveOut))
            {
                query = DbspNet.Sql.Optimizer.PlanColumnLiveness.PruneDeadColumns(query, liveOut);
            }

            var blockers = ParallelBlockers(DbspNet.Sql.Optimizer.PlanOptimizer.Optimize(query));
            if (blockers.Count > 0)
            {
                result.Add((v.ViewName, blockers));
            }
        }

        return result;
    }
}
