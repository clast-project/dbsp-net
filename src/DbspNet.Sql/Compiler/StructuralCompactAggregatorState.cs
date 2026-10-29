// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Expressions;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Compile-time switch for the structural path's MIN/MAX per-group state
/// (docs/design-state-representation.md §4B.3, the structural port of variant B2).
/// <c>DBSPNET_STRUCT_MINMAX=legacy</c> forces the old state process-wide.
/// </summary>
internal static class StructuralCompactAggregatorStateMode
{
    private static readonly bool EnvironmentLegacy = string.Equals(
        Environment.GetEnvironmentVariable("DBSPNET_STRUCT_MINMAX")?.Trim(), "legacy", StringComparison.OrdinalIgnoreCase);

    // Null = the default (see MinMaxDefault).
    [ThreadStatic]
    private static StructuralMinMaxStateKind? _minMax;

    /// <summary>The default when nothing has been set on this thread.</summary>
    internal const StructuralMinMaxStateKind MinMaxDefault = StructuralMinMaxStateKind.Hybrid;

    internal static StructuralMinMaxStateKind MinMax
    {
        get => _minMax ?? (EnvironmentLegacy ? StructuralMinMaxStateKind.Legacy : MinMaxDefault);
        set => _minMax = value;
    }

    /// <summary>The aggregator the structural compilers build for MIN/MAX.</summary>
    internal static SqlAggregator BuildMinMax(Func<StructuralRow, object?> argExtract, bool wantMin)
        => MinMax == StructuralMinMaxStateKind.Hybrid
            ? new SqlMinMaxHybridAggregator(argExtract, wantMin)
            : new SqlMinMaxAggregator(argExtract, wantMin);
}

/// <summary>Per-group state of a structural MIN/MAX.</summary>
internal enum StructuralMinMaxStateKind
{
    /// <summary><c>Dictionary&lt;object,long&gt;</c> + <c>SortedSet&lt;object&gt;</c> per group.</summary>
    Legacy,

    /// <summary>(best, count) until the group's best is first retracted, then one rescan
    /// promotes the group to the legacy sorted state.</summary>
    Hybrid,
}

/// <summary>
/// Structural MIN/MAX whose per-group state starts as the current best value and the
/// number of positive-weight rows holding it — one small object, instead of a
/// <c>Dictionary</c> + <c>SortedSet</c> that duplicates the operator's trace. The first
/// tick that retracts the last holder of the best (or finds the state inconsistent)
/// rescans the post-delta group once into the legacy sorted state, which the group keeps
/// until it empties. Same row-transition semantics as <see cref="SqlMinMaxAggregator"/>:
/// a value is eligible while some positive-weight row holds it. Values are matched by
/// <see cref="IComparable.CompareTo"/>, so two values that compare equal but are not
/// <c>Equals</c> count as one holder (the typed hybrid's rule).
/// </summary>
internal sealed class SqlMinMaxHybridAggregator : SqlAggregator
{
    private readonly Func<StructuralRow, object?> _argExtract;
    private readonly bool _wantMin;
    private readonly SqlMinMaxAggregator _promoted;

    public SqlMinMaxHybridAggregator(Func<StructuralRow, object?> argExtract, bool wantMin)
    {
        _argExtract = argExtract;
        _wantMin = wantMin;
        _promoted = new SqlMinMaxAggregator(argExtract, wantMin);
    }

    private sealed class Compact
    {
        public object? Best;
        public long Count;   // positive-weight rows holding Best; 0 = no best
    }

    private bool Better(object v, object best)
    {
        var cmp = ((IComparable)v).CompareTo(best);
        return _wantMin ? cmp < 0 : cmp > 0;
    }

    public override object? Compute(IMultiset<StructuralRow, Z64> rows) => _promoted.Compute(rows);

    public override object? Update(
        ref object? state,
        object? oldValue,
        ZSet<StructuralRow, Z64> delta,
        IMultiset<StructuralRow, Z64> after)
    {
        if (state is not null and not Compact)
        {
            // Promoted: the legacy state, O(log n) per change. Demote once empty.
            var result = _promoted.Update(ref state, oldValue, delta, after);
            if (result is null)
            {
                state = null;
            }

            return result;
        }

        var s = state as Compact ?? new Compact();
        foreach (var (row, w) in delta)
        {
            var v = _argExtract(row);
            if (v is null)
            {
                continue;
            }

            var afterW = after.WeightOf(row).Value;
            var wasPositive = afterW - w.Value > 0;
            var isPositive = afterW > 0;
            if (wasPositive == isPositive)
            {
                continue;
            }

            if (isPositive)
            {
                if (s.Count == 0 || Better(v, s.Best!))
                {
                    s.Best = v;
                    s.Count = 1;
                }
                else if (((IComparable)v).CompareTo(s.Best) == 0)
                {
                    s.Count++;
                }
            }
            else if (s.Count == 0 || Better(v, s.Best!)
                     || (((IComparable)v).CompareTo(s.Best) == 0 && --s.Count == 0))
            {
                // Lost the extreme (or the state is inconsistent): the next best is only
                // in the trace. Rescan once into the sorted state the group keeps.
                object? promoted = null;
                var positive = PositiveRows(after);
                var result = _promoted.Update(ref promoted, oldValue, positive, positive);
                state = result is null ? null : promoted;
                return result;
            }
        }

        state = s.Count > 0 ? s : null;
        return s.Count > 0 ? s.Best : null;
    }

    // Folding the group's positive-weight rows as a delta whose "after" is itself
    // (so every row's before-weight is 0) rebuilds exactly the legacy state: each row
    // transitions from absent to positive. Non-positive rows never hold a value.
    private static ZSet<StructuralRow, Z64> PositiveRows(IMultiset<StructuralRow, Z64> after)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (row, w) in after)
        {
            if (Z64.IsPositive(w))
            {
                b.Add(row, w);
            }
        }

        return b.Build();
    }
}
