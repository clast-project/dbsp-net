// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// A rough per-plan row-count estimate, used only by the broadcast-join heuristic
/// to tell a small dimension from a large fact — <b>not</b> a general-purpose
/// optimizer cardinality model. Base relations get their supplied count (a scan
/// of an unknown name estimates to <see cref="Unknown"/> so it is never mistaken
/// for small); the rest propagate a deliberately simple, detection-oriented
/// estimate — filters/projects pass through, an inner join keys one side into the
/// other (max of the sides), a set op sums, an aggregate/distinct collapses to at
/// most its input, and anything unhandled is <see cref="Unknown"/>. Every
/// arithmetic step saturates at <see cref="Unknown"/> so a large subtree never
/// wraps around to a small number.
/// </summary>
internal static class CardinalityEstimator
{
    /// <summary>The estimate for a relation whose size cannot be determined —
    /// large by construction, so it is never treated as a broadcastable dimension.</summary>
    internal const long Unknown = long.MaxValue;

    /// <summary>
    /// Estimate the row count of <paramref name="plan"/>, resolving base-relation /
    /// referenced-view sizes through <paramref name="lookup"/> (name → count, or
    /// <c>null</c> if unknown).
    /// </summary>
    public static long Estimate(LogicalPlan plan, Func<string, long?> lookup)
    {
        switch (plan)
        {
            case ScanPlan s:
                return lookup(s.TableName) ?? Unknown;

            case CteScanPlan c:
                return Estimate(c.Cte.Plan, lookup);

            case FilterPlan f:
                return Estimate(f.Input, lookup); // a filter only shrinks — its input is a safe upper bound.

            case ProjectPlan p:
                return Estimate(p.Input, lookup);

            case TemporalFilterPlan tf:
                return Estimate(tf.Input, lookup);

            case DistinctPlan d:
                return Estimate(d.Input, lookup); // distinct rows <= input rows.

            case AggregatePlan a:
                return Estimate(a.Input, lookup); // one row per group <= input rows.

            case JoinPlan j:
                // Key-join detection estimate: matching rows key one side into the
                // other, so max is a reasonable dimension-vs-fact discriminator (a
                // dimension joined to small refs stays small; a fact join inherits
                // the fact's size). Not a bound for fan-out joins — acceptable here.
                return SatMax(Estimate(j.Left, lookup), Estimate(j.Right, lookup));

            case UnionAllPlan u:
                var total = 0L;
                foreach (var b in u.Branches)
                {
                    total = SatAdd(total, Estimate(b, lookup));
                }

                return total;

            case DifferencePlan diff:
                return Estimate(diff.Left, lookup); // L − R <= L.

            case PartitionedTopKPlan pt:
                return Estimate(pt.Input, lookup);

            case PartitionedRankPlan pr:
                return Estimate(pr.Input, lookup);

            case WindowAggregatePlan wa:
                return Estimate(wa.Input, lookup);

            case WindowOffsetPlan wo:
                return Estimate(wo.Input, lookup);

            case TopKPlan t:
                return Estimate(t.Input, lookup);

            default:
                return Unknown; // semi-join, scalar/correlated subquery, recursive CTE, …
        }
    }

    private static long SatMax(long a, long b) => a == Unknown || b == Unknown ? Unknown : Math.Max(a, b);

    private static long SatAdd(long a, long b)
    {
        if (a == Unknown || b == Unknown)
        {
            return Unknown;
        }

        var sum = a + b;
        return sum < 0 ? Unknown : sum; // overflow → treat as large.
    }
}
