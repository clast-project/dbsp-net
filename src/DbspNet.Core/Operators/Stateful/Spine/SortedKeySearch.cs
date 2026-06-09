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

    /// <summary>
    /// Galloping (exponential) search for <paramref name="value"/> in
    /// <c>sorted[from..]</c>, returning the index if present, else the bitwise
    /// complement of the insertion point. The caller must pass a
    /// <paramref name="from"/> cursor that is a valid lower bound — i.e. every
    /// <paramref name="value"/> handed in across a sweep is non-decreasing, so
    /// <c>sorted[from-1] &lt; value</c>. Probing M non-decreasing keys against an
    /// array of length L this way costs O(M·log(L/M)) rather than the
    /// O(M·log L) of M independent <see cref="IndexOf{T}"/> calls — this is the
    /// merge-vs-point-probe win the spine arrangement makes available (see
    /// docs/design-row-representation.md §6.1). Primitive keys take a
    /// specialised native-compare body; other T defer to the comparer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GallopIndexOf<T>(T[] sorted, int from, T value, IComparer<T> comparer)
    {
        if (typeof(T) == typeof(int))
        {
            return GallopInt((int[])(object)sorted, from, (int)(object)value!);
        }

        if (typeof(T) == typeof(long))
        {
            return GallopLong((long[])(object)sorted, from, (long)(object)value!);
        }

        return GallopGeneric(sorted, from, value, comparer);
    }

    private static int GallopInt(int[] sorted, int from, int value)
    {
        int n = sorted.Length;
        if (from >= n)
        {
            return ~n;
        }

        // Expand a window to the right until sorted[curr] >= value, keeping
        // prev as the last index known to be < value.
        int prev = from - 1;
        int curr = from;
        int step = 1;
        while (curr < n)
        {
            int m = sorted[curr];
            if (m == value)
            {
                return curr;
            }

            if (m > value)
            {
                break;
            }

            prev = curr;
            curr = from + step;
            step <<= 1;
        }

        int lo = prev + 1;
        int hi = curr < n ? curr : n - 1;
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

    private static int GallopLong(long[] sorted, int from, long value)
    {
        int n = sorted.Length;
        if (from >= n)
        {
            return ~n;
        }

        int prev = from - 1;
        int curr = from;
        int step = 1;
        while (curr < n)
        {
            long m = sorted[curr];
            if (m == value)
            {
                return curr;
            }

            if (m > value)
            {
                break;
            }

            prev = curr;
            curr = from + step;
            step <<= 1;
        }

        int lo = prev + 1;
        int hi = curr < n ? curr : n - 1;
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

    private static int GallopGeneric<T>(T[] sorted, int from, T value, IComparer<T> comparer)
    {
        int n = sorted.Length;
        if (from >= n)
        {
            return ~n;
        }

        int prev = from - 1;
        int curr = from;
        int step = 1;
        while (curr < n)
        {
            int c = comparer.Compare(sorted[curr], value);
            if (c == 0)
            {
                return curr;
            }

            if (c > 0)
            {
                break;
            }

            prev = curr;
            curr = from + step;
            step <<= 1;
        }

        int lo = prev + 1;
        int hi = curr < n ? curr : n - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int c = comparer.Compare(sorted[mid], value);
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
