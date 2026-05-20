// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace Clast.BloomFilter;

/// <summary>
/// Produces a 64-bit hash of values of type <typeparamref name="T"/>.
/// The hash should be approximately uniformly distributed across the
/// full 64-bit space — both halves are used by
/// <see cref="SplitBlockBloomFilter"/> (upper 32 bits select the
/// block, lower 32 bits select bits within it).
/// </summary>
/// <remarks>
/// Implementations need not be stable across processes — typical
/// bloom filter use rebuilds the filter from source data on load.
/// They MUST be stable within a single process's lifetime: if a value
/// hashes to <c>h</c> at insert time, it must still hash to <c>h</c>
/// at probe time, otherwise the filter loses its no-false-negative
/// guarantee.
/// </remarks>
public interface IHash64<T>
{
    /// <summary>Computes the 64-bit hash of <paramref name="value"/>.</summary>
    ulong Hash(T value);
}
