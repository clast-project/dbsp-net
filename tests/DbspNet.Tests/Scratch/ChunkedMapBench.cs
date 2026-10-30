// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Diagnostics;
using System.Globalization;
using DbspNet.Core.Collections;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

/// <summary>
/// <c>CHUNKED_MAP_BENCH=1</c>: <see cref="ChunkedMap{TKey,TValue}"/> vs <see cref="Dictionary{TKey,TValue}"/>
/// on StructuralRow keys — builder-style add (TryGetValue then set), lookup, enumerate — at
/// several sizes, alternating, median of 9 (docs/design-chunked-deltas.md §5 gate: ≤ 5% slower).
/// </summary>
public class ChunkedMapBench(ITestOutputHelper output)
{
    [Fact]
    public void Compare()
    {
        if (Environment.GetEnvironmentVariable("CHUNKED_MAP_BENCH") != "1")
        {
            return;
        }

        foreach (var n in new[] { 10, 1_000, 100_000, 1_000_000 })
        {
            var keys = Enumerable.Range(0, n).Select(i => new StructuralRow((long)i, "k" + (i % 97))).ToArray();
            var reps = Math.Max(1, 2_000_000 / n);
            var d = new List<double>[3];
            var c = new List<double>[3];
            for (var k = 0; k < 3; k++)
            {
                d[k] = [];
                c[k] = [];
            }

            for (var round = 0; round < 9; round++)
            {
                Run(d, () => new Dictionary<StructuralRow, long>(), (m, key) =>
                {
                    if (m.TryGetValue(key, out var w))
                    {
                        m[key] = w + 1;
                    }
                    else
                    {
                        m[key] = 1;
                    }
                }, (m, key) => m.TryGetValue(key, out _), m => { long s = 0; foreach (var kv in m) { s += kv.Value; } return s; }, keys, reps);
                Run(c, () => new ChunkedMap<StructuralRow, long>(), (m, key) =>
                {
                    if (m.TryGetValue(key, out var w))
                    {
                        m[key] = w + 1;
                    }
                    else
                    {
                        m[key] = 1;
                    }
                }, (m, key) => m.TryGetValue(key, out _), m => { long s = 0; foreach (var kv in m) { s += kv.Value; } return s; }, keys, reps);
            }

            string[] names = ["add", "lookup", "enumerate"];
            for (var k = 0; k < 3; k++)
            {
                var dm = Median(d[k]);
                var cm = Median(c[k]);
                output.WriteLine(string.Create(
                    CultureInfo.InvariantCulture,
                    $"BENCH n={n,9:N0} {names[k],-9} dictionary {dm,7:F2} ns/op  chunked {cm,7:F2} ns/op  ratio {cm / dm:F3}"));
            }
        }
    }

    private static void Run<TMap>(
        List<double>[] into,
        Func<TMap> create,
        Action<TMap, StructuralRow> add,
        Func<TMap, StructuralRow, bool> lookup,
        Func<TMap, long> enumerate,
        StructuralRow[] keys,
        int reps)
    {
        double addNs = 0, lookupNs = 0, enumNs = 0;
        long sink = 0;
        for (var r = 0; r < reps; r++)
        {
            var sw = Stopwatch.StartNew();
            var m = create();
            foreach (var k in keys)
            {
                add(m, k);
            }

            addNs += sw.Elapsed.TotalNanoseconds;
            sw.Restart();
            foreach (var k in keys)
            {
                sink += lookup(m, k) ? 1 : 0;
            }

            lookupNs += sw.Elapsed.TotalNanoseconds;
            sw.Restart();
            sink += enumerate(m);
            enumNs += sw.Elapsed.TotalNanoseconds;
        }

        var ops = (double)reps * keys.Length;
        into[0].Add(addNs / ops);
        into[1].Add(lookupNs / ops);
        into[2].Add(enumNs / ops);
        GC.KeepAlive(sink);
    }

    private static double Median(List<double> xs)
    {
        var s = xs.Order().ToList();
        return s[s.Count / 2];
    }
}
