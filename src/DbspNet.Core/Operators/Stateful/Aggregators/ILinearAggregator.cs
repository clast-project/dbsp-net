// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// An aggregate whose per-group state is a function of the group's <b>integrated
/// weights alone</b>: running weighted sums (COUNT, SUM, AVG) and per-value summed
/// weights (COUNT(DISTINCT)). Such an aggregate never needs to see the group's rows
/// again, so <see cref="LinearAggregateOp{TKey,TValue,TOut}"/> keeps no input trace
/// (DBSP's linear aggregation; docs/design-state-representation.md §4G).
/// </summary>
/// <remarks>
/// Contract: after any sequence of <see cref="Fold"/> calls the state depends only on
/// the sum of the deltas folded (for an integer accumulator, exactly), so a group
/// whose integrated contents are the empty Z-set reports <see cref="IsZero"/>.
/// </remarks>
public interface ILinearAggregator<TValue, TOut>
    where TValue : notnull
    where TOut : notnull
{
    /// <summary>
    /// Fold one group's <paramref name="delta"/> into <paramref name="state"/>
    /// (<c>null</c> on a group's first tick; the aggregator allocates it).
    /// </summary>
    void Fold(ref object? state, ZSet<TValue, Z64> delta);

    /// <summary>The aggregate value. Called only while the group's summed row weight is non-zero.</summary>
    TOut Result(object? state);

    /// <summary>
    /// True when the state is the zero accumulator, i.e. folding the group's rows
    /// has cancelled out (integer weights all zero). The operator drops a group only
    /// when this holds and its summed row weight is zero.
    /// </summary>
    bool IsZero(object? state);

    /// <summary>Serialise one group's state verbatim (floating-point bits included).</summary>
    void WriteState(System.Buffers.IBufferWriter<byte> writer, object? state);

    /// <summary>Inverse of <see cref="WriteState"/>.</summary>
    object? ReadState(ReadOnlySpan<byte> blob);
}
