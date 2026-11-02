// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
//
// FREEZE_BENCH=1: where BlockedIndexedTrace.Freeze's time goes for one 1M-entry managed level
// (docs/design-flat-trace-state.md §3) — consolidation, the hash-ordered entry list and its sort,
// and block encoding — on the FlatTraceProbeBench row shape.
using System.Diagnostics;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;
using Schema = DbspNet.Sql.Plan.Schema;
using SchemaColumn = DbspNet.Sql.Plan.SchemaColumn;

namespace DbspNet.Tests.Scratch;

public class FreezePhaseBench(ITestOutputHelper output)
{
    [Fact]
    public void Run()
    {
        if (Environment.GetEnvironmentVariable("FREEZE_BENCH") != "1")
        {
            return;
        }

        var keySchema = new Schema([new SchemaColumn("k", new SqlBigintType(false))]);
        var valueSchema = new Schema([
            new SchemaColumn("id", new SqlBigintType(false)), new SchemaColumn("qty", new SqlIntegerType(true)),
            new SchemaColumn("name", new SqlVarcharType(null, true)), new SchemaColumn("code", new SqlVarcharType(null, true)),
            new SchemaColumn("price", new SqlDoubleType(true)), new SchemaColumn("d", new SqlDateType(true)),
            new SchemaColumn("amt", new SqlDecimalType(18, 2, true)), new SchemaColumn("note", new SqlVarcharType(null, true)),
        ]);
        var format = BlockSchema.TryCreate(keySchema, valueSchema)!;
        var rnd = new Random(1);
        var names = Enumerable.Range(0, 50_000).Select(i => (object)Utf8String.Of("customer-name-" + i)).ToArray();
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        long id = 0;
        for (var k = 0; k < 250_000; k++)
        {
            var key = new StructuralRow((long)k * 7919);
            for (var f = 0; f < 4; f++)
            {
                b.Add(key, new StructuralRow(id++, rnd.Next(1000), names[rnd.Next(names.Length)], names[rnd.Next(64)],
                    rnd.NextDouble(), new Date32(18000 + rnd.Next(3000)), new Decimal128((Int128)rnd.Next(1_000_000)), Utf8String.Of("note-" + id)), Z64.One);
            }
        }

        var level = b.Build();
        var threads = int.Parse(Environment.GetEnvironmentVariable("FREEZE_THREADS") ?? "1", System.Globalization.CultureInfo.InvariantCulture);
        for (var round = 0; round < 4; round++)
        {
            var lines = new string[threads];
            Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t => lines[t] = Once(level, format));
            foreach (var line in lines)
            {
                output.WriteLine(line);
            }
        }
    }

    private static string Once(IndexedZSet<StructuralRow, StructuralRow, Z64> level, BlockSchema format)
    {
        {
            var sw = Stopwatch.StartNew();
            var c = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            foreach (var (key, group) in level)
            {
                foreach (var (value, weight) in group)
                {
                    c.Add(key, value, weight);
                }
            }

            var consolidated = c.Build();
            var tConsolidate = sw.Elapsed.TotalMilliseconds;
            sw.Restart();
            var entries = new List<(uint Hash, int ValueHash, StructuralRow Key, StructuralRow Value, long Weight)>();
            foreach (var (key, group) in consolidated)
            {
                var hash = (uint)key.GetHashCode();
                foreach (var (value, weight) in group)
                {
                    entries.Add((hash, value.GetHashCode(), key, value, weight.Value));
                }
            }

            var tList = sw.Elapsed.TotalMilliseconds;
            sw.Restart();
            entries.Sort(static (x, y) => x.Hash != y.Hash ? x.Hash.CompareTo(y.Hash) : x.ValueHash.CompareTo(y.ValueHash));
            var tSort = sw.Elapsed.TotalMilliseconds;
            sw.Restart();
            var size = int.Parse(Environment.GetEnvironmentVariable("FREEZE_BLOCK") ?? "1024", System.Globalization.CultureInfo.InvariantCulture);
            var keys = new StructuralRow[size];
            var values = new StructuralRow[size];
            var weights = new long[size];
            var hashes = new int[size];
            var n = 0;
            var blocks = 0;
            for (var i = 0; i < entries.Count; i++)
            {
                keys[n] = entries[i].Key;
                values[n] = entries[i].Value;
                weights[n] = entries[i].Weight;
                hashes[n] = (int)entries[i].Hash;
                n++;
                if (n == size || i == entries.Count - 1)
                {
                    format.Encode(keys, values, weights, hashes, n);
                    blocks++;
                    n = 0;
                }
            }

            var tEncode = sw.Elapsed.TotalMilliseconds;
            return FormattableString.Invariant(
                $"FREEZE entries={entries.Count} consolidate={tConsolidate:F0}ms list={tList:F0}ms sort={tSort:F0}ms encode={tEncode:F0}ms ({blocks} blocks) per_entry_ns={(tConsolidate + tList + tSort + tEncode) * 1e6 / entries.Count:F0}");
        }
    }
}
