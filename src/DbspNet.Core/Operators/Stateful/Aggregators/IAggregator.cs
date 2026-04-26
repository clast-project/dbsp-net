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
    Optional<TOut> Compute(ZSet<TValue, Z64> multiset);

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
        ZSet<TValue, Z64> afterMultiset)
        => Compute(afterMultiset);
}
