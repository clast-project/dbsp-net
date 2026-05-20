// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.IO.Hashing;
using System.Runtime.InteropServices;

namespace Clast.BloomFilter;

/// <summary>
/// Provides a default <see cref="IHash64{T}"/> for a given type,
/// mirroring the shape of <see cref="EqualityComparer{T}.Default"/>.
/// </summary>
/// <remarks>
/// Specializations are provided for the common primitive types
/// (<see cref="int"/>, <see cref="long"/>, <see cref="uint"/>,
/// <see cref="ulong"/>, <see cref="string"/>, <see cref="Guid"/>).
/// For any other type the fallback hashes via
/// <see cref="EqualityComparer{T}.Default"/> then mixes with
/// splitmix64; the bloom FPP for that path is bounded by the
/// quality of <c>GetHashCode</c>.
/// </remarks>
public static class Hash64
{
    /// <summary>Returns the default <see cref="IHash64{T}"/> for <typeparamref name="T"/>.</summary>
    public static IHash64<T> Default<T>() => Hash64Default<T>.Instance;
}

internal static class Hash64Default<T>
{
    public static readonly IHash64<T> Instance = Create();

    private static IHash64<T> Create()
    {
        var t = typeof(T);
        if (t == typeof(int))    return (IHash64<T>)(object)new Int32Hash64();
        if (t == typeof(long))   return (IHash64<T>)(object)new Int64Hash64();
        if (t == typeof(uint))   return (IHash64<T>)(object)new UInt32Hash64();
        if (t == typeof(ulong))  return (IHash64<T>)(object)new UInt64Hash64();
        if (t == typeof(string)) return (IHash64<T>)(object)new StringHash64();
        if (t == typeof(Guid))   return (IHash64<T>)(object)new GuidHash64();
        return new FallbackHash64<T>();
    }
}

internal sealed class Int32Hash64 : IHash64<int>
{
    public ulong Hash(int value) => SplitMix64.Mix((ulong)(uint)value);
}

internal sealed class Int64Hash64 : IHash64<long>
{
    public ulong Hash(long value) => SplitMix64.Mix((ulong)value);
}

internal sealed class UInt32Hash64 : IHash64<uint>
{
    public ulong Hash(uint value) => SplitMix64.Mix(value);
}

internal sealed class UInt64Hash64 : IHash64<ulong>
{
    public ulong Hash(ulong value) => SplitMix64.Mix(value);
}

internal sealed class StringHash64 : IHash64<string>
{
    public ulong Hash(string value)
    {
        if (value is null) return 0;
        return XxHash64.HashToUInt64(MemoryMarshal.AsBytes(value.AsSpan()));
    }
}

internal sealed class GuidHash64 : IHash64<Guid>
{
    public ulong Hash(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes);
        return XxHash64.HashToUInt64(bytes);
    }
}

internal sealed class FallbackHash64<T> : IHash64<T>
{
    private readonly EqualityComparer<T> _eq = EqualityComparer<T>.Default;

    public ulong Hash(T value)
    {
        int h = value is null ? 0 : _eq.GetHashCode(value);
        return SplitMix64.Mix((uint)h);
    }
}
