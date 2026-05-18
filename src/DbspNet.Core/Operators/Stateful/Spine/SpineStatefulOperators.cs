using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// <see cref="CircuitBuilder"/> extension methods for the spine-backed
/// stateful operators. Each method is the spine counterpart of an
/// existing flat-trace builder in
/// <see cref="StatefulOperators"/>; both can coexist in the same
/// circuit, which keeps phase-2 A/B benchmarks honest.
/// </summary>
public static class SpineStatefulOperators
{
    /// <summary>
    /// Incremental <c>distinct</c> backed by an LSM-style
    /// <see cref="SpineZSetTrace{TKey,TWeight}"/>. Observable behaviour
    /// matches <see cref="StatefulOperators.Distinct"/>; the difference
    /// is purely in how the running trace is stored.
    /// </summary>
    /// <param name="compactionStrategy">
    /// Optional compaction policy; defaults to
    /// <see cref="TieredCompactionStrategy.Default"/> (4 batches per
    /// level).
    /// </param>
    public static Stream<ZSet<TKey, TWeight>> SpineDistinct<TKey, TWeight>(
        this CircuitBuilder builder,
        Stream<ZSet<TKey, TWeight>> input,
        ICompactionStrategy? compactionStrategy = null,
        IZSetTraceCodec<TKey, TWeight>? snapshotCodec = null,
        IComparer<TKey>? keyComparer = null)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(input);

        var output = new Stream<ZSet<TKey, TWeight>>(ZSet<TKey, TWeight>.Empty);
        builder.AddRawOperator(
            new SpineDistinctOp<TKey, TWeight>(input, output, compactionStrategy, snapshotCodec, keyComparer));
        return output;
    }
}
