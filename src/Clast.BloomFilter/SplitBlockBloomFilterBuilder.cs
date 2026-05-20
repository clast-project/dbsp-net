// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Clast.BloomFilter;

/// <summary>
/// Builder for <see cref="SplitBlockBloomFilter"/>. Insert values, then
/// either materialize a probe-only <see cref="Build"/> filter or hand
/// the raw bitset bytes to a serializer.
/// </summary>
public sealed class SplitBlockBloomFilterBuilder
{
    private const int BytesPerBlock = SplitBlockBloomFilter.BytesPerBlock;

    private readonly byte[] _data;
    private readonly int _numBlocks;

    /// <summary>Creates a builder with the given bitset size.</summary>
    /// <param name="numBytes">Filter size in bytes. Must be a positive multiple of 32.</param>
    public SplitBlockBloomFilterBuilder(int numBytes)
    {
        if (numBytes <= 0 || numBytes % BytesPerBlock != 0)
            throw new ArgumentException(
                $"Filter size must be a positive multiple of {BytesPerBlock}, got {numBytes}.",
                nameof(numBytes));

        _data = new byte[numBytes];
        _numBlocks = numBytes / BytesPerBlock;
    }

    /// <summary>Filter size in bytes.</summary>
    public int NumBytes => _data.Length;

    /// <summary>
    /// Computes the optimal filter size in bytes for a given number of distinct
    /// values and target false-positive probability, capped at <paramref name="maxBytes"/>
    /// and rounded up to an SBBF block boundary.
    /// </summary>
    public static int OptimalNumBytes(int ndv, double fpp, int maxBytes)
    {
        if (ndv <= 0) return BytesPerBlock;

        double ln2 = Math.Log(2);
        double optimalBits = -ndv * Math.Log(fpp) / (ln2 * ln2);
        int optimalBytes = Math.Max(BytesPerBlock, (int)Math.Ceiling(optimalBits / 8.0));

        optimalBytes = ((optimalBytes + BytesPerBlock - 1) / BytesPerBlock) * BytesPerBlock;

        return Math.Min(optimalBytes, maxBytes);
    }

    /// <summary>Inserts a plain-encoded value.</summary>
    public void Add(ReadOnlySpan<byte> value)
    {
        ulong hash = XxHash64.HashToUInt64(value);
        AddHash(hash);
    }

    /// <summary>Inserts a pre-hashed value. The 64-bit hash should be uniformly distributed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddHash(ulong hash)
    {
        uint upper = (uint)(hash >> 32);
        int blockIndex = (int)(((ulong)upper * (ulong)_numBlocks) >> 32);
        uint key = (uint)hash;

        SbbfBlock.BlockInsert(_data, blockIndex * BytesPerBlock, key);
    }

    /// <summary>Returns the raw bitset bytes (live reference). Use for serialization.</summary>
    public byte[] ToArray() => _data;

    /// <summary>Materializes a probe-only <see cref="SplitBlockBloomFilter"/> over the current bitset.</summary>
    public SplitBlockBloomFilter Build() => new(_data);
}
