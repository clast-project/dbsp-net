// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Deterministic, process-independent hashing for partition keys. Unlike
/// <see cref="object.GetHashCode"/> — which for <see cref="string"/> is
/// randomized per process — these values depend only on the input, so the same
/// key maps to the same shard across runs and across a snapshot/recovery cycle.
/// That stability is required for data-parallel sharding: a recovered circuit
/// must place each key on the same worker it held before.
/// </summary>
/// <remarks>
/// Integer keys use MurmurHash3's finalizer; strings use FNV-1a over their
/// UTF-16 code units (allocation-free). The results are well-distributed but are
/// <em>not</em> cryptographic and carry no cross-language guarantee — they only
/// need to be stable for this implementation.
/// </remarks>
public static class StableHash
{
    private const ulong FnvOffset = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    /// <summary>Stable hash of a 32-bit integer (MurmurHash3 fmix32).</summary>
    public static int Of(int value)
    {
        unchecked
        {
            var h = (uint)value;
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return (int)h;
        }
    }

    /// <summary>Stable hash of a 64-bit integer (MurmurHash3 fmix64, folded to 32 bits).</summary>
    public static int Of(long value)
    {
        unchecked
        {
            var h = (ulong)value;
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdUL;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53UL;
            h ^= h >> 33;
            return (int)(h ^ (h >> 32));
        }
    }

    /// <summary>Stable hash of a string (FNV-1a over UTF-16 code units, folded to 32 bits).</summary>
    public static int Of(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        unchecked
        {
            var h = FnvOffset;
            foreach (var c in value)
            {
                h = (h ^ (byte)c) * FnvPrime;
                h = (h ^ (byte)(c >> 8)) * FnvPrime;
            }

            return (int)(h ^ (h >> 32));
        }
    }

    /// <summary>Stable hash of a byte span (FNV-1a, folded to 32 bits).</summary>
    public static int Of(ReadOnlySpan<byte> value)
    {
        unchecked
        {
            var h = FnvOffset;
            foreach (var b in value)
            {
                h = (h ^ b) * FnvPrime;
            }

            return (int)(h ^ (h >> 32));
        }
    }

    /// <summary>
    /// Combine component hashes into one stable hash (FNV-1a over the components),
    /// for composite keys such as multi-column SQL group/join keys. Order matters.
    /// </summary>
    public static int Combine(params int[] hashes)
    {
        ArgumentNullException.ThrowIfNull(hashes);
        unchecked
        {
            var h = FnvOffset;
            foreach (var part in hashes)
            {
                h = (h ^ (uint)part) * FnvPrime;
            }

            return (int)(h ^ (h >> 32));
        }
    }
}
