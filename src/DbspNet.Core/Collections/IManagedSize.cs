// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Collections;

/// <summary>
/// A row value that can say roughly how many managed bytes it holds, boxed in a row
/// (docs/design-bounded-memory.md §5.8: estimating a managed cache against a byte budget). Values
/// that do not implement it are estimated by type.
/// </summary>
public interface IManagedSize
{
    /// <summary>Approximate managed bytes of the boxed value and what it alone references.</summary>
    int ManagedBytes { get; }
}

/// <summary>Rough managed-size estimates for rows (64-bit runtime; headers included).</summary>
internal static class ManagedSize
{
    /// <summary>A row: the <see cref="StructuralRow"/> object, its value array and each value.</summary>
    public static long Row(StructuralRow row)
    {
        long n = 32 + 24 + (8L * row.Count);
        for (var i = 0; i < row.Count; i++)
        {
            n += Value(row[i]);
        }

        return n;
    }

    private static int Value(object? value) => value switch
    {
        null => 0,
        string s => 22 + (2 * s.Length),
        byte[] b => 24 + b.Length,
        IManagedSize m => m.ManagedBytes,
        _ => 24, // a boxed primitive, date or decimal (decimal is 32; close enough)
    };

    /// <summary>The mean of <see cref="Row"/> over up to <paramref name="sample"/> rows spread across
    /// <paramref name="count"/>.</summary>
    public static long MeanRow(int count, Func<int, StructuralRow> rowAt, int sample = 32)
    {
        if (count == 0)
        {
            return 0;
        }

        var take = Math.Min(sample, count);
        long total = 0;
        for (var k = 0; k < take; k++)
        {
            total += Row(rowAt((int)((long)k * count / take)));
        }

        return total / take;
    }
}
