// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Binary search over a sorted <typeparamref name="T"/>[] with
/// per-primitive specialisations. The <c>typeof(T) ==</c> tests
/// JIT-fold to compile-time constants for value-type instantiations,
/// so for <see cref="int"/>, <see cref="long"/>, <see cref="string"/>
/// the generic body collapses to the inlinable hand-rolled loop with
/// no virtual dispatch per comparison. Other T values defer to
/// <see cref="Array.BinarySearch{T}(T[], T, IComparer{T})"/>.
/// </summary>
/// <remarks>
/// The wider spine path probes ~log(N) keys per batch × ~8 batches
/// passing the bloom. With <c>IComparer&lt;T&gt;</c> dispatch each
/// comparison is a virtual call (~5 ns); for primitive keys the
/// inlined loop runs at ~1-2 ns per comparison. This is the
/// canonical sorted-LSM optimisation — see the phase 2 benchmark
/// regression note in benchmarks.md.
/// </remarks>
internal static class SortedKeySearch
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int IndexOf<T>(T[] sorted, T value, IComparer<T> comparer)
    {
        if (typeof(T) == typeof(int))
        {
            return IndexOfInt((int[])(object)sorted, (int)(object)value!);
        }

        if (typeof(T) == typeof(long))
        {
            return IndexOfLong((long[])(object)sorted, (long)(object)value!);
        }

        if (typeof(T) == typeof(uint))
        {
            return IndexOfUInt((uint[])(object)sorted, (uint)(object)value!);
        }

        if (typeof(T) == typeof(ulong))
        {
            return IndexOfULong((ulong[])(object)sorted, (ulong)(object)value!);
        }

        if (typeof(T) == typeof(string))
        {
            return IndexOfStringOrdinal((string[])(object)sorted, (string)(object)value!);
        }

        return Array.BinarySearch(sorted, value, comparer);
    }

    private static int IndexOfInt(int[] sorted, int value)
    {
        int lo = 0, hi = sorted.Length - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int m = sorted[mid];
            if (m == value)
            {
                return mid;
            }

            if (m < value)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    private static int IndexOfLong(long[] sorted, long value)
    {
        int lo = 0, hi = sorted.Length - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            long m = sorted[mid];
            if (m == value)
            {
                return mid;
            }

            if (m < value)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    private static int IndexOfUInt(uint[] sorted, uint value)
    {
        int lo = 0, hi = sorted.Length - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            uint m = sorted[mid];
            if (m == value)
            {
                return mid;
            }

            if (m < value)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    private static int IndexOfULong(ulong[] sorted, ulong value)
    {
        int lo = 0, hi = sorted.Length - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            ulong m = sorted[mid];
            if (m == value)
            {
                return mid;
            }

            if (m < value)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }

    private static int IndexOfStringOrdinal(string[] sorted, string value)
    {
        int lo = 0, hi = sorted.Length - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int c = string.CompareOrdinal(sorted[mid], value);
            if (c == 0)
            {
                return mid;
            }

            if (c < 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return ~lo;
    }
}
