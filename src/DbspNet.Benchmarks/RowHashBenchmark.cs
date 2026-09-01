// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Benchmarks;

/// <summary>
/// Head-to-head cost of the row hash itself: the shipped <see cref="StructuralRowHash"/> against
/// the <see cref="HashCode"/> formulation it replaced, over identical cell arrays, in one process.
/// <c>w1profile</c> prices the change in situ but its ns/event is noisy; this isolates the term.
/// </summary>
internal static class RowHashBenchmark
{
    public static void Run(int rows, int runs)
    {
        var wide = BuildRows(rows, wide: true);
        var narrow = BuildRows(rows, wide: false);

        Console.WriteLine($"=== row hash A/B (rows={rows:N0}, median of {runs}) ===");
        Report("wide  (long,Utf8,double,Date32,Decimal128,null,bool)", wide, runs);
        Report("narrow(long,long)", narrow, runs);

        // Split: per-cell seed dispatch vs the combiner around it.
        Console.WriteLine("  cell-seed dispatch only (no combine):");
        Split("wide", wide, runs);
        Split("narrow", narrow, runs);

        // Same wide row minus the Decimal128 column: isolates what the one type whose own
        // GetHashCode is seeded costs us to hash deterministically.
        var noDec = BuildRows(rows, wide: true, decimals: false);
        Split("wide-nodec", noDec, runs);
    }

    private static void Report(string label, object?[][] data, int runs)
    {
        var neu = new double[runs];
        var old = new double[runs];
        var arr = new double[runs];
        for (var r = 0; r < runs; r++)
        {
            // Alternate so drift hits both arms equally.
            neu[r] = Time(data, New);
            old[r] = Time(data, Old);
            arr[r] = Time(data, NewArray);
        }

        Array.Sort(neu);
        Array.Sort(old);
        Array.Sort(arr);
        var n = neu[runs / 2];
        var o = old[runs / 2];
        var a = arr[runs / 2];
        Console.WriteLine($"  {label}");
        Console.WriteLine(
            $"    HashCode (was) {o,7:F1}   StructuralRowHash {n,7:F1} ({(n - o) / o * 100,+5:F1}%)   + array overload {a,7:F1} ({(a - o) / o * 100,+5:F1}%)  ns/row");
    }

    private static void Split(string label, object?[][] data, int runs)
    {
        var neu = new double[runs];
        var old = new double[runs];
        for (var r = 0; r < runs; r++)
        {
            neu[r] = Time(data, CellsOnlyNew);
            old[r] = Time(data, CellsOnlyOld);
        }

        Array.Sort(neu);
        Array.Sort(old);
        var n = neu[runs / 2];
        var o = old[runs / 2];
        Console.WriteLine(
            $"    {label,-6} GetHashCode {o,7:F1} ns/row   Cell() {n,7:F1} ns/row   {(n - o) / o * 100,+6:F1}%");
    }

    private static int CellsOnlyNew(object?[] values)
    {
        var acc = 0UL;
        for (var i = 0; i < values.Length; i++)
        {
            acc ^= StructuralRowHash.Cell(values[i]);
        }

        return (int)acc;
    }

    private static int CellsOnlyOld(object?[] values)
    {
        var acc = 0;
        for (var i = 0; i < values.Length; i++)
        {
            acc ^= values[i]?.GetHashCode() ?? 0;
        }

        return acc;
    }

    private static double Time(object?[][] data, Func<object?[], int> hash)
    {
        var sink = 0;
        var sw = Stopwatch.StartNew();
        foreach (var row in data)
        {
            sink ^= hash(row);
        }

        sw.Stop();
        GC.KeepAlive(sink);
        return sw.Elapsed.TotalMilliseconds * 1e6 / data.Length;
    }

    private static int New(object?[] values) => StructuralRow.ComputeHash((IReadOnlyList<object?>)values);

    // Through IReadOnlyList, exactly as StructuralRow.ComputeHash is called: an interface
    // dispatch per cell. Indexing the array directly here instead would flatter this arm and
    // charge the difference to the algorithm.
    private static int Old(object?[] array) => OldCore(array);

#pragma warning disable CA1859 // the interface IS the thing under test here
    private static int OldCore(IReadOnlyList<object?> values)
    {
        var hc = default(HashCode);
        hc.Add(values.Count);
        for (var i = 0; i < values.Count; i++)
        {
            hc.Add(values[i]);
        }

        return hc.ToHashCode();
    }
#pragma warning restore CA1859

    // What the array overload buys: same algorithm, no interface dispatch.
    private static int NewArray(object?[] values) => StructuralRow.ComputeHash(values);

    private static object?[][] BuildRows(int rows, bool wide, bool decimals = true)
    {
        var data = new object?[rows][];
        for (var i = 0; i < rows; i++)
        {
            data[i] = wide
                ? new object?[]
                {
                    (long)i,
                    Utf8String.Of("SYMBOL" + (i % 997)),
                    i * 1.5,
                    new Date32(19000 + (i % 365)),
                    decimals ? new Decimal128((System.Int128)(i * 100)) : (object)(long)i,
                    null,
                    i % 2 == 0,
                }
                : new object?[] { (long)i, (long)(i % 1024) };
        }

        return data;
    }
}
