// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace Clast.BloomFilter;

/// <summary>
/// Typed probe-only bloom filter. Wraps a
/// <see cref="SplitBlockBloomFilter"/> and an <see cref="IHash64{T}"/>
/// to give a clean <c>MightContain(T)</c> surface without forcing
/// callers to think about hashing or byte-level encoding.
/// </summary>
public sealed class BloomFilter<T>
{
    private readonly SplitBlockBloomFilter _inner;
    private readonly IHash64<T> _hash;

    /// <summary>Wraps an existing byte-level filter with the given hash function.</summary>
    public BloomFilter(SplitBlockBloomFilter inner, IHash64<T>? hash = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _hash = hash ?? Hash64.Default<T>();
    }

    /// <summary>The underlying byte-level filter, for serialization.</summary>
    public SplitBlockBloomFilter Inner => _inner;

    /// <summary>Tests whether <paramref name="value"/> might be present.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MightContain(T value) => _inner.MightContainHash(_hash.Hash(value));
}
