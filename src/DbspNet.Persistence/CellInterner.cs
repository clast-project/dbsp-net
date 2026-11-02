// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;

namespace DbspNet.Persistence;

/// <summary>
/// Shares equal boxed cells within one decoded column, so a restored row points at the same box
/// as its neighbours the way an ingested row shares its projection's cells
/// (docs/design-durable-state.md, "Giant groups decoded without sharing"). Without it every
/// decoded cell is a box of its own.
/// </summary>
/// <remarks>
/// Only representation-exact equality is used: doubles and floats by their bits (so 0.0 and -0.0,
/// and distinct NaN payloads, stay distinct), decimals never (1.0m equals 1.00m but prints
/// differently), everything else by its own <c>Equals</c>, which for the engine's cell types
/// (integers, booleans, <c>Utf8String</c>, dates, timestamps, intervals) is value identity.
/// <para><b>Opt-in</b> (<c>DBSPNET_DECODE_INTERN=1</c>). On ivm-bench SF=3 W=12 it cut what the
/// first batch after a fresh restart retains from 1.55 GB / 41.0M objects (arena strings alone) to
/// 0.75 GB / 11.8M, but the interning slowed the background giant-group decode enough that the
/// drain waited on it: drain median 1223 ms against 877 ms for arena strings alone (5 alternating
/// runs each), with no smaller GC pause. Worth turning on where restored memory matters more than
/// first-batch latency.</para>
/// </remarks>
internal static class CellInterner
{
    internal static bool Enabled { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_DECODE_INTERN") == "1";

    /// <summary>Columns shorter than this are left alone: a cold read of one key's few rows pays a
    /// dictionary per column for almost no sharing (measured slower on the first batch after
    /// restore). <c>DBSPNET_DECODE_INTERN_MIN</c> overrides.</summary>
    internal static int MinRows { get; set; } =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_DECODE_INTERN_MIN"), out var m) ? m : 256;

    /// <summary>Replace each cell in <paramref name="column"/> by the first equal box seen.</summary>
    public static void Intern(object?[] column)
    {
        if (!Enabled || column.Length < MinRows)
        {
            return;
        }

        Dictionary<object, object>? seen = null;
        for (var i = 0; i < column.Length; i++)
        {
            var v = column[i];
            if (v is null or decimal)
            {
                continue;
            }

            seen ??= new Dictionary<object, object>(Math.Min(column.Length, 1024), ExactComparer.Instance);
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(seen, v, out var exists);
            if (exists)
            {
                column[i] = slot;
            }
            else
            {
                slot = v;
            }
        }
    }

    private sealed class ExactComparer : IEqualityComparer<object>
    {
        public static readonly ExactComparer Instance = new();

        public new bool Equals(object? x, object? y) => (x, y) switch
        {
            (double a, double b) => BitConverter.DoubleToInt64Bits(a) == BitConverter.DoubleToInt64Bits(b),
            (float a, float b) => BitConverter.SingleToInt32Bits(a) == BitConverter.SingleToInt32Bits(b),
            _ => x!.GetType() == y!.GetType() && x.Equals(y),
        };

        public int GetHashCode(object obj) => obj switch
        {
            double d => BitConverter.DoubleToInt64Bits(d).GetHashCode(),
            float f => BitConverter.SingleToInt32Bits(f),
            _ => obj.GetHashCode(),
        };
    }
}
