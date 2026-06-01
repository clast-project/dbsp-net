// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// A monotonically non-decreasing lower bound on a stream's future values,
/// advertised to stateful operators so they can garbage-collect unreachable
/// trace state. If the operator's state is keyed on a monotone value, any key
/// strictly below <see cref="Value"/> can never be touched by a future input
/// (future values are guaranteed to be at or above the bound), so that state
/// is safe to drop without affecting output.
/// </summary>
/// <remarks>
/// This is the "frontier" / "waterline" of the DBSP / Differential Dataflow
/// literature, reduced to a single <see cref="long"/> on the monotone column's
/// ordering key (temporal types are <see cref="long"/>/<see cref="int"/> under
/// the hood; a logical-time column is a <see cref="long"/> directly). The value
/// is read once per tick, after the operator integrates its delta.
/// </remarks>
public interface IFrontier
{
    /// <summary>
    /// The current bound. State keyed on a monotone value strictly less than
    /// this is unreachable by future input and may be collected.
    /// <see cref="long.MinValue"/> means no bound has been advertised yet —
    /// nothing is collectable.
    /// </summary>
    long Value { get; }
}

/// <summary>
/// A trivially settable <see cref="IFrontier"/> whose value only ever moves
/// forward. The input-side frontier source (and tests) drive it via
/// <see cref="AdvanceTo"/>.
/// </summary>
public sealed class MutableFrontier : IFrontier
{
    public long Value { get; private set; } = long.MinValue;

    /// <summary>
    /// Advance the bound to <paramref name="value"/>. Never moves the frontier
    /// backward — a lower value is ignored, preserving monotonicity.
    /// </summary>
    public void AdvanceTo(long value)
    {
        if (value > Value)
        {
            Value = value;
        }
    }
}

/// <summary>
/// The minimum of several frontiers — the GC bound for a column fed by multiple
/// <c>LATENESS</c> sources (e.g. a join or union of differently-late inputs). A
/// key is collectable only when it is below <em>every</em> contributing
/// frontier, so the combined bound is the min; if any source has not advanced
/// (<see cref="long.MinValue"/>), nothing is collected.
/// </summary>
public sealed class MinFrontier : IFrontier
{
    private readonly IReadOnlyList<IFrontier> _sources;

    public MinFrontier(IReadOnlyList<IFrontier> sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        _sources = sources;
    }

    public long Value
    {
        get
        {
            if (_sources.Count == 0)
            {
                return long.MinValue;
            }

            var min = long.MaxValue;
            foreach (var f in _sources)
            {
                if (f.Value < min)
                {
                    min = f.Value;
                }
            }

            return min;
        }
    }
}

/// <summary>
/// A frontier whose value is an inner frontier passed through a monotone
/// non-decreasing transform. Used when a GC key is a monotone <em>function</em>
/// of a <c>LATENESS</c> column rather than the column itself: e.g. a group key
/// of <c>date_trunc('day', ts)</c> lives in a different value space than the raw
/// <c>ts</c> frontier, so the bound must be transformed by the same function
/// (<c>date_trunc('day', maxSeen − lateness)</c>) before it can threshold the
/// derived keys. The transform must be non-decreasing for the bound to stay
/// sound. <see cref="long.MinValue"/> (no bound yet) passes through untouched.
/// </summary>
public sealed class TransformedFrontier : IFrontier
{
    private readonly IFrontier _inner;
    private readonly Func<long, long> _transform;

    public TransformedFrontier(IFrontier inner, Func<long, long> transform)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(transform);
        _inner = inner;
        _transform = transform;
    }

    public long Value
    {
        get
        {
            var v = _inner.Value;
            return v == long.MinValue ? long.MinValue : _transform(v);
        }
    }
}
