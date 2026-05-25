// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
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
