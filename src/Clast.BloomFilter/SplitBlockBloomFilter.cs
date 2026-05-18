// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Clast.BloomFilter;

/// <summary>
/// Parquet Split Block Bloom Filter (SBBF). Each block is 256 bits
/// (8 × uint32); a probe touches a single block so the entire filter
/// query fits in one cache line. Hashing is xxHash64.
/// </summary>
/// <remarks>
/// This is the byte-level surface — what gets persisted to a file
/// format. For probing arbitrary typed keys in-process, prefer the
/// <see cref="BloomFilter{T}"/> facade.
/// </remarks>
public sealed class SplitBlockBloomFilter
{
    internal const int BytesPerBlock = 32;

    private readonly byte[] _data;
    private readonly int _numBlocks;

    /// <summary>Creates a bloom filter over the given raw bitset data.</summary>
    /// <param name="data">Filter bitset. Length must be a positive multiple of 32.</param>
    public SplitBlockBloomFilter(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        if (data.Length == 0 || data.Length % BytesPerBlock != 0)
            throw new ArgumentException(
                $"Bloom filter data length must be a positive multiple of {BytesPerBlock}, got {data.Length}.",
                nameof(data));

        _data = data;
        _numBlocks = data.Length / BytesPerBlock;
    }

    /// <summary>Returns the underlying bitset bytes (live reference, do not mutate).</summary>
    public ReadOnlySpan<byte> Data => _data;

    /// <summary>Tests whether the given plain-encoded value might be present.</summary>
    public bool MightContain(ReadOnlySpan<byte> value)
    {
        ulong hash = XxHash64.HashToUInt64(value);
        return MightContainHash(hash);
    }

    /// <summary>Tests whether a pre-hashed value might be present. The 64-bit hash should be uniformly distributed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MightContainHash(ulong hash)
    {
        uint upper = (uint)(hash >> 32);
        int blockIndex = (int)(((ulong)upper * (ulong)_numBlocks) >> 32);
        uint key = (uint)hash;

        return SbbfBlock.BlockProbe(_data, blockIndex * BytesPerBlock, key);
    }
}
