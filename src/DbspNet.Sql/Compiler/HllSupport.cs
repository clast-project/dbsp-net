// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Shared plumbing for the <c>APPROX_COUNT_DISTINCT</c> aggregators on both
/// compile paths: a value-stable 64-bit hash over the runtime SQL value types
/// (<see cref="HllHashing"/>) and a generic "fold the positive-weight,
/// non-null values of a multiset into a sketch" helper (<see cref="FoldPositive"/>).
/// </summary>
internal static class HllSupport
{
    /// <summary>
    /// Fold every distinct value carried by a positive-weight, non-null row of
    /// <paramref name="rows"/> into <paramref name="sketch"/>. NULL-valued rows
    /// are skipped (SQL <c>COUNT(DISTINCT)</c> ignores NULL); weight magnitude
    /// is irrelevant because <see cref="HyperLogLog.AddHash"/> is idempotent —
    /// a value present in any positive-weight row counts exactly once.
    /// </summary>
    public static void FoldPositive<TRow>(
        HyperLogLog sketch, IMultiset<TRow, Z64> rows, Func<TRow, object?> argExtract)
        where TRow : notnull
    {
        foreach (var (row, weight) in rows)
        {
            if (!Z64.IsPositive(weight))
            {
                continue;
            }

            var value = argExtract(row);
            if (value is null)
            {
                continue;
            }

            sketch.AddHash(HllHashing.Hash(value));
        }
    }
}

/// <summary>
/// Maps a boxed runtime SQL value to a well-distributed 64-bit hash for
/// HyperLogLog bucketing. The mapping is value-based and deterministic within
/// a process: every type the SQL runtime can produce as a scalar value is
/// handled explicitly, so the estimate never depends on .NET's per-process
/// randomized <see cref="string.GetHashCode()"/> (VARCHAR values arrive as
/// <see cref="Utf8String"/> and are hashed over their bytes).
/// </summary>
internal static class HllHashing
{
    public static ulong Hash(object value)
    {
        // Each branch derives a 64-bit seed from the value's content, then runs
        // it through the SplitMix64 finalizer so even low-entropy seeds (small
        // integers, booleans) spread across the full 64-bit space.
        var seed = value switch
        {
            long l => (ulong)l,
            int i => (ulong)(long)i,
            short s => (ulong)(long)s,
            byte b => b,
            bool flag => flag ? 1UL : 0UL,
            double d => DoubleSeed(d),
            float f => DoubleSeed(f),
            Decimal128 dec => DecimalSeed(dec),
            Utf8String s => BytesSeed(s.Span),
            string s => BytesSeed(System.Text.Encoding.UTF8.GetBytes(s)),
            Date32 d => (ulong)(long)d.Days,
            Time64 t => (ulong)t.Microseconds,
            Timestamp ts => (ulong)ts.Microseconds,
            Interval iv => IntervalSeed(iv),
            // Every scalar runtime type the resolver can hand us is covered
            // above. A new value type reaching here is a wiring gap; surface it
            // loudly rather than silently degrade the estimate.
            _ => throw new NotSupportedException(
                $"APPROX_COUNT_DISTINCT cannot hash a value of type {value.GetType()}"),
        };

        return Mix(seed);
    }

    private static ulong DoubleSeed(double value)
    {
        // Collapse -0.0 to +0.0 so the two compare-equal zeros hash alike.
        if (value == 0.0)
        {
            value = 0.0;
        }

        return (ulong)BitConverter.DoubleToInt64Bits(value);
    }

    private static ulong DecimalSeed(Decimal128 value)
    {
        // Within one column every value shares a scale, so equal values share a
        // mantissa; hashing the 128-bit mantissa (both halves) distinguishes them.
        var mantissa = value.Mantissa;
        var low = (ulong)mantissa;
        var high = (ulong)(mantissa >> 64);
        return low ^ RotateLeft(high, 32);
    }

    private static ulong IntervalSeed(Interval value) =>
        (ulong)value.Micros ^ RotateLeft((ulong)(long)value.Months, 32);

    private static ulong BytesSeed(ReadOnlySpan<byte> bytes)
    {
        // FNV-1a/64 over the bytes; the SplitMix64 finalizer in Hash does the
        // avalanche, so a simple accumulator here is sufficient.
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offsetBasis;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }

        return hash;
    }

    private static ulong RotateLeft(ulong value, int offset) =>
        (value << offset) | (value >> (64 - offset));

    private static ulong Mix(ulong x)
    {
        // SplitMix64 finalizer.
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9UL;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBUL;
        x ^= x >> 31;
        return x;
    }
}
