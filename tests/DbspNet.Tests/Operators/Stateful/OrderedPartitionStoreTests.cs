// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// <see cref="OrderedPartitionStore"/> (docs/design-flat-trace-state.md §3.15) against a model of
/// each partition's rows: random batches of inserts, deletes and partial cancellations, with tiny
/// promotion and chunk sizes so partitions move to chunks, chunks split, merge away and empty. Every
/// read must be ordered and consolidated, cover its range, and — when it carries a prefix partial —
/// that partial must equal a fold over exactly the rows below what was read.
/// </summary>
public class OrderedPartitionStoreTests
{
    private static readonly Schema KeySchema = new([new SchemaColumn("p", new SqlIntegerType(false))]);

    private static readonly Schema RowSchema = new([
        new SchemaColumn("v", new SqlBigintType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
        new SchemaColumn("p", new SqlIntegerType(false)),
    ]);

    // An exact test aggregate: (row count, sum of v), weighted.
    private sealed class CountSum : IPartialAggregator<StructuralRow, StructuralRow>
    {
        public bool IsExactPartial => true;

        public object NewAccumulator() => new long[2];

        public void Add(object accumulator, StructuralRow row, long weight)
        {
            var a = (long[])accumulator;
            a[0] += weight;
            a[1] += weight * (long)row[0]!;
        }

        public void CombineInto(object accumulator, object other)
        {
            var a = (long[])accumulator;
            var o = (long[])other;
            a[0] += o[0];
            a[1] += o[1];
        }

        public object Clone(object accumulator) => ((long[])accumulator).Clone();

        public Optional<StructuralRow> Result(object accumulator)
        {
            var a = (long[])accumulator;
            return Optional<StructuralRow>.Some(new StructuralRow(a[0], a[1]));
        }
    }

    private static int Compare(StructuralRow a, StructuralRow b) => StructuralRowComparer.Instance.Compare(a, b);

    [Theory]
    [InlineData(1, 3, 2)]
    [InlineData(2, 3, 1)]
    [InlineData(3, 6, 4)]
    [InlineData(4, 1, 1)]
    [InlineData(5, 50, 8)]
    public void Reads_MatchTheModel(int seed, int promoteAt, int chunkRows)
    {
        var rnd = new Random(seed);
        var partial = new CountSum();
        var store = new OrderedPartitionStore(
            BlockSchema.TryCreate(Schema.Empty, RowSchema)!,
            row => (long)row[0]!,
            StructuralRowComparer.Instance,
            partial,
            promoteAt: promoteAt,
            chunkRows: chunkRows);
        // Enough partitions that new ones keep arriving (read from the tick's own rows, §3.17).
        var partitions = 3 + (seed * 7);
        var model = new Dictionary<int, Dictionary<StructuralRow, long>>();
        for (var p = 0; p < partitions; p++)
        {
            model[p] = new();
        }

        string?[] strings = [null, "a", "bb", "é"];
        for (var batch = 0; batch < 80; batch++)
        {
            if (rnd.Next(40) == 0)
            {
                store.Clear(); // every key new again
                foreach (var m in model.Values)
                {
                    m.Clear();
                }
            }

            var delta = new Dictionary<(int P, StructuralRow Row), long>();
            for (var n = rnd.Next(1, 12); n > 0; n--)
            {
                var p = rnd.Next(partitions);
                StructuralRow row;
                long w;
                if (model[p].Count > 0 && rnd.Next(3) == 0)
                {
                    // Retract (all or part of) a live row.
                    var (r, live) = model[p].ElementAt(rnd.Next(model[p].Count));
                    row = r;
                    w = -rnd.Next(1, (int)live + 1);
                }
                else
                {
                    // Few distinct values, so peer groups grow and chunks cut between them.
                    row = new StructuralRow((long)rnd.Next(0, 15), strings[rnd.Next(strings.Length)] is { } s ? Utf8String.Of(s) : null, p);
                    w = rnd.Next(1, 3);
                }

                delta[(p, row)] = delta.GetValueOrDefault((p, row)) + w;
            }

            var changes = new List<(StructuralRow Key, StructuralRow Row, long Weight)>();
            foreach (var ((p, row), w) in delta)
            {
                if (w == 0 || model[p].GetValueOrDefault(row) + w < 0)
                {
                    continue; // keep every live weight positive, as a window's input is
                }

                changes.Add((new StructuralRow(p), row, w));
                var next = model[p].GetValueOrDefault(row) + w;
                if (next == 0)
                {
                    model[p].Remove(row);
                }
                else
                {
                    model[p][row] = next;
                }
            }

            store.Integrate(changes);
            if (rnd.Next(4) == 0)
            {
                store.Freeze();
            }

            for (var p = 0; p < partitions; p++)
            {
                var expected = model[p].OrderBy(e => e.Key, Comparer<StructuralRow>.Create(Compare)).ToList();
                long from = rnd.Next(-2, 17), to = rnd.Next(-2, 17);
                if (from > to)
                {
                    (from, to) = (to, from);
                }

                var needPrefix = rnd.Next(2) == 0;
                var rows = store.Read(new StructuralRow(p), from, to, needPrefix, out var before);

                // Ordered, consolidated, every weight the model's.
                for (var i = 1; i < rows.Count; i++)
                {
                    Assert.True(Compare(rows[i - 1].Row, rows[i].Row) < 0, $"batch {batch} p {p}: out of order at {i}");
                }

                foreach (var (row, weight, value) in rows)
                {
                    Assert.Equal(model[p].GetValueOrDefault(row), weight);
                    Assert.Equal((long)row[0]!, value);
                }

                // Covers its range: every model row with a value in [from, to] was read.
                foreach (var (row, _) in expected.Where(e => (long)e.Key[0]! >= from && (long)e.Key[0]! <= to))
                {
                    Assert.Contains(rows, r => r.Row.Equals(row));
                }

                if (needPrefix)
                {
                    // Everything below what was read is in the prefix partial, or the read starts
                    // at the partition's first row.
                    var firstRead = rows.Count > 0 ? rows[0].Row : null;
                    // (Nothing read: every chunk from the one reaching `from` lies above `to`, so
                    // the prefix is exactly the rows below `from`.)
                    var below = firstRead is null
                        ? expected.Where(e => (long)e.Key[0]! < from).ToList()
                        : expected.TakeWhile(e => Compare(e.Key, firstRead) < 0).ToList();
                    if (before is null)
                    {
                        Assert.Empty(below);
                    }
                    else
                    {
                        var acc = partial.NewAccumulator();
                        foreach (var (row, weight) in below)
                        {
                            partial.Add(acc, row, weight);
                        }

                        Assert.Equal(partial.Result(acc), partial.Result(before));
                    }
                }
            }

            // The whole state, for a full write.
            var all = store.All().GroupBy(e => e.Row).ToDictionary(g => g.Key, g => g.Sum(e => e.Weight));
            Assert.Equal(model.Values.Sum(m => m.Count), all.Count(e => e.Value != 0));
            foreach (var m in model.Values)
            {
                foreach (var (row, weight) in m)
                {
                    Assert.Equal(weight, all.GetValueOrDefault(row));
                }
            }
        }

        Assert.True(promoteAt > 40 || store.LargePartitions > 0, "no partition was promoted");
    }
}
