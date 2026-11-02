// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Text;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// <see cref="OutputRowIndex"/> (docs/design-flat-trace-state.md §3.9): both representations against
/// a model of which addresses each row holds. Which copies a take returns is the index's choice, so
/// the model checks that they were live copies of that row, and that what is left matches.
/// </summary>
public sealed class OutputRowIndexTests
{
    private static readonly Schema ViewSchema = new([
        new SchemaColumn("a", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
        new SchemaColumn("d", new SqlDoubleType(true)),
    ]);

    private static StructuralRow Row(int i)
    {
        // Few distinct rows, so copies pile up into large groups that span blocks; a NaN with a
        // non-default payload must still find its own copies.
        var d = (i % 4) switch
        {
            0 => (object?)null,
            1 => BitConverter.Int64BitsToDouble(0x7FF8_0000_0000_0000L | (uint)i),
            _ => (double)i,
        };
        return new StructuralRow(i % 5, i % 3 == 0 ? null : Utf8String.FromBytes(Encoding.UTF8.GetBytes("row" + i)), d);
    }

    public static TheoryData<int, bool, int, int> Cases() => new()
    {
        { 1, false, 0, 0 },
        { 2, false, 0, 0 },
        { 1, true, 1, 1 },
        { 2, true, 3, 2 },
        { 3, true, 4, 2 },
        { 4, true, 16, 8 },
        { 5, true, 1 << 16, 1024 },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void MatchesModel(int seed, bool blocked, int freezeEntries, int blockEntries)
    {
        var rng = new Random(seed);
        var index = blocked
            ? OutputRowIndex.CreateBlocked(ViewSchema, freezeEntries, blockEntries)
            : OutputRowIndex.CreateManaged();
        Assert.Equal(blocked, index.IsBlocked);
        var rows = Enumerable.Range(0, 40).Select(Row).ToArray();
        var model = new Dictionary<StructuralRow, HashSet<long>>();
        long next = 0;

        for (var batch = 0; batch < 60; batch++)
        {
            if (rng.Next(25) == 0)
            {
                index.Clear();
                model.Clear();
            }

            // Each row at most once per batch, either taken from or added to (as the runner does).
            // Rows are fresh copies, so lookups go by value, not by reference.
            var touched = new HashSet<int>();
            for (var op = rng.Next(1, 30); op > 0; op--)
            {
                var r = rng.Next(rows.Length);
                if (!touched.Add(r))
                {
                    continue;
                }

                var row = Row(r);
                model.TryGetValue(row, out var live);
                if (live is { Count: > 0 } && rng.Next(2) == 0)
                {
                    var count = rng.Next(1, live.Count + 1);
                    var taken = new List<long>();
                    index.Take(row, count, taken);
                    Assert.Equal(count, taken.Count);
                    foreach (var address in taken)
                    {
                        Assert.True(live.Remove(address), $"batch {batch}: took {address}, not a live copy of row {r}");
                    }
                }
                else
                {
                    for (var n = rng.Next(1, 6); n > 0; n--)
                    {
                        index.Add(row, next);
                        (model.TryGetValue(row, out var set) ? set : model[row] = new HashSet<long>()).Add(next);
                        next++;
                    }
                }
            }

            index.Commit();
            Assert.Equal(model.Values.Sum(s => s.Count), index.Count);
            foreach (var r in rows)
            {
                var expected = model.TryGetValue(r, out var set) ? set.Order() : Enumerable.Empty<long>();
                Assert.Equal(expected, index.AddressesOf(Row(Array.IndexOf(rows, r))).Order());
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TakingMoreThanLive_Throws(bool blocked)
    {
        var index = blocked ? OutputRowIndex.CreateBlocked(ViewSchema, 2, 1) : OutputRowIndex.CreateManaged();
        index.Add(Row(1), 10);
        index.Add(Row(1), 11);
        index.Commit();
        Assert.Throws<InvalidOperationException>(() => index.Take(Row(1), 3, new List<long>()));
        Assert.Throws<InvalidOperationException>(() => (blocked ? index : OutputRowIndex.CreateManaged()).Take(Row(2), 1, new List<long>()));
    }

    [Fact]
    public void Create_IsBlockedOnlyWhenViewsAreBlocksAndColumnsEncode()
    {
        Assert.False(OutputRowIndex.Create(ViewSchema).IsBlocked);
        using (TraceBlocksMode.Use(join: false, view: true))
        {
            Assert.True(OutputRowIndex.Create(ViewSchema).IsBlocked);
            var interval = new Schema([new SchemaColumn("i", new SqlIntervalType(IntervalQualifier.Day, false))]);
            Assert.False(OutputRowIndex.Create(interval).IsBlocked);
        }
    }
}
