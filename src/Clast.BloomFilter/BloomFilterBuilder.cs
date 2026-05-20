// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace Clast.BloomFilter;

/// <summary>
/// Typed builder for a <see cref="BloomFilter{T}"/>. Insert values with
/// <see cref="Add"/>, then call <see cref="Build"/> for a probe-only
/// filter (or grab the raw bytes from the underlying builder for
/// serialization).
/// </summary>
public sealed class BloomFilterBuilder<T>
{
    private readonly SplitBlockBloomFilterBuilder _inner;
    private readonly IHash64<T> _hash;

    /// <summary>Creates a builder for a bloom filter sized to <paramref name="numBytes"/>.</summary>
    public BloomFilterBuilder(int numBytes, IHash64<T>? hash = null)
        : this(new SplitBlockBloomFilterBuilder(numBytes), hash)
    {
    }

    /// <summary>Creates a builder wrapping an existing byte-level builder.</summary>
    public BloomFilterBuilder(SplitBlockBloomFilterBuilder inner, IHash64<T>? hash = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _hash = hash ?? Hash64.Default<T>();
    }

    /// <summary>
    /// Sizes a bloom filter for an expected number of distinct values and target
    /// false-positive probability. See
    /// <see cref="SplitBlockBloomFilterBuilder.OptimalNumBytes"/>.
    /// </summary>
    public static BloomFilterBuilder<T> WithCapacity(int expectedDistinct, double fpp, int maxBytes, IHash64<T>? hash = null)
        => new(SplitBlockBloomFilterBuilder.OptimalNumBytes(expectedDistinct, fpp, maxBytes), hash);

    /// <summary>The underlying byte-level builder.</summary>
    public SplitBlockBloomFilterBuilder Inner => _inner;

    /// <summary>Inserts <paramref name="value"/> into the filter.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(T value) => _inner.AddHash(_hash.Hash(value));

    /// <summary>Materializes a probe-only <see cref="BloomFilter{T}"/>.</summary>
    public BloomFilter<T> Build() => new(_inner.Build(), _hash);
}
