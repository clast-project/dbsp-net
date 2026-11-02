// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// An aggregate that can be computed from partial results over the pieces of a multiset, combined
/// in any grouping, with a result <b>equal</b> to <see cref="IAggregator{TValue,TOut}.Compute"/> over
/// the whole and to the incremental fold (docs/design-flat-trace-state.md §3.15). Exactness is the
/// point: the window operator recomputes the output it emitted earlier (§3.11), so a partial that
/// differed in the last bits (a floating-point sum combined in a new order) would leave a
/// retraction that does not cancel. Only exact aggregates (COUNT, MIN, MAX) implement it.
/// </summary>
/// <remarks>Accumulators are mutable and owned by the caller: <see cref="Add"/> and
/// <see cref="CombineInto"/> change the first argument, <see cref="Clone"/> copies.</remarks>
internal interface IPartialAggregator<TRow, TOut>
    where TRow : notnull
    where TOut : notnull
{
    /// <summary>Whether every part of this aggregate combines exactly; when false the partial
    /// operations must not be used.</summary>
    bool IsExactPartial { get; }

    /// <summary>The accumulator of the empty multiset.</summary>
    object NewAccumulator();

    /// <summary>Fold <paramref name="row"/> at <paramref name="weight"/> into <paramref name="accumulator"/>.</summary>
    void Add(object accumulator, TRow row, long weight);

    /// <summary>Fold everything <paramref name="other"/> holds into <paramref name="accumulator"/>.</summary>
    void CombineInto(object accumulator, object other);

    /// <summary>A copy of <paramref name="accumulator"/> that later changes do not share.</summary>
    object Clone(object accumulator);

    /// <summary>The aggregate of what <paramref name="accumulator"/> holds — equal to
    /// <see cref="IAggregator{TValue,TOut}.Compute"/> over the same multiset.</summary>
    Optional<TOut> Result(object accumulator);
}
