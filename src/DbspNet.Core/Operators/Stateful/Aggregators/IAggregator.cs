// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// An aggregate function. Given the per-group multiset of values (only
/// positive-weight entries are semantically meaningful — negative-weight
/// entries represent retractions that would have been cancelled out under
/// correct SQL semantics), produce a single result or <see cref="Optional{T}.None"/>
/// for an empty group (matching SQL's SUM/AVG/MIN/MAX-on-empty behaviour).
/// </summary>
public interface IAggregator<TValue, TOut>
    where TValue : notnull
    where TOut : notnull
{
    /// <summary>
    /// Compute the aggregate from scratch over <paramref name="multiset"/>.
    /// Non-optional for every aggregator — the fallback path when there is
    /// no incremental state available.
    /// </summary>
    Optional<TOut> Compute(IMultiset<TValue, Z64> multiset);

    /// <summary>
    /// Produce the new aggregate value given the prior cached value
    /// (<paramref name="oldValue"/>), the incoming <paramref name="delta"/> for
    /// this group, and the post-delta multiset (<paramref name="afterMultiset"/>).
    /// <paramref name="state"/> is opaque per-aggregator scratch space that
    /// survives across ticks; on first call for a group it is <c>null</c> and
    /// the aggregator may allocate or leave it as <c>null</c>.
    /// Aggregators that are not exactly incrementalizable (e.g. MIN/MAX) may
    /// inherit the default, which simply scans <paramref name="afterMultiset"/>.
    /// </summary>
    Optional<TOut> Update(
        ref object? state,
        Optional<TOut> oldValue,
        ZSet<TValue, Z64> delta,
        IMultiset<TValue, Z64> afterMultiset)
        => Compute(afterMultiset);

    /// <summary>
    /// True when some part of this aggregator's per-group state must be persisted
    /// verbatim across a snapshot, because re-deriving it by folding the restored
    /// group does not reproduce what an uninterrupted run held.
    /// </summary>
    /// <remarks>
    /// <para>Default <c>false</c>: an aggregator whose fold is exact (integer SUM,
    /// COUNT, MIN/MAX — anything associative over its accumulator) needs nothing,
    /// because folding the restored group lands on the same state either way.</para>
    /// <para>Floating-point accumulators are the exception, and the reason this
    /// exists. <c>+=</c> over <c>double</c> is not associative, so a bulk fold of a
    /// restored group can differ in the last bits from the incremental per-tick fold
    /// that built the live state. The operator then retracts a value the downstream
    /// view never held, the retraction fails to cancel, and the view keeps both rows.
    /// See <c>docs/design-incremental-persistence.md</c> §7.2.</para>
    /// </remarks>
    bool CanPersistState => false;

    /// <summary>
    /// Serialise one group's <paramref name="state"/>. Only called when
    /// <see cref="CanPersistState"/> is true.
    /// </summary>
    void WriteState(System.Buffers.IBufferWriter<byte> writer, object? state)
        => throw new NotSupportedException(
            $"{GetType().Name} does not persist state; check CanPersistState first.");

    /// <summary>
    /// Merge a blob written by <see cref="WriteState"/> into <paramref name="state"/>,
    /// which the caller has <b>already rebuilt by folding the restored group</b>.
    /// </summary>
    /// <remarks>
    /// Merge rather than replace, so a composite aggregate can restore only the slots
    /// that need it and leave the exactly-reconstructible ones as folded. That matters
    /// for a mixed group such as <c>SELECT AVG(x), MIN(y) … GROUP BY k</c>, where an
    /// all-or-nothing contract would drop the blob entirely and let the AVG drift.
    /// </remarks>
    void MergePersistedState(ref object? state, ReadOnlySpan<byte> blob)
        => throw new NotSupportedException(
            $"{GetType().Name} does not persist state; check CanPersistState first.");
}
