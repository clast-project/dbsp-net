// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// The output row index under an evicting block cache (docs/design-bounded-memory.md §3.9): after
/// each batch its runs are written to scratch files and rebound, so the cache can drop them; it
/// still answers exactly like the model, and keeps one file per live run.
/// </summary>
[Collection(nameof(BlockedCheckpointTests))]
public class OutputRowIndexSpillTests
{
    private static void WithCache(Action test)
    {
        var (saved, savedAlign) = (BlockCache.Budget, BlockAccess.AlignSpans);
        BlockCache.Budget = 1;
        BlockAccess.AlignSpans = true;
        try
        {
            test();
        }
        finally
        {
            BlockCache.Budget = saved;
            BlockAccess.AlignSpans = savedAlign;
            BlockCache.Clear();
        }
    }

    [Theory]
    [InlineData(1, 1, 1)]
    [InlineData(2, 3, 2)]
    [InlineData(3, 4, 2)]
    [InlineData(4, 16, 8)]
    public void MatchesModel_WithEveryBlockEvicted(int seed, int freezeEntries, int blockEntries) =>
        WithCache(() => new OutputRowIndexTests().MatchesModel(seed, true, freezeEntries, blockEntries));

    [Fact]
    public void Runs_AreFileBacked_OneFilePerLiveRun() => WithCache(() =>
    {
        var schema = new Schema([new SchemaColumn("k", new SqlIntegerType(false)), new SchemaColumn("s", new SqlVarcharType(null, true))]);
        var index = OutputRowIndex.CreateBlocked(schema, freezeEntries: 4, blockEntries: 2);
        long next = 0;
        for (var batch = 0; batch < 40; batch++)
        {
            for (var i = 0; i < 7; i++)
            {
                index.Add(new StructuralRow((batch * 7) + i, Utf8String.Of("r" + i)), next++);
            }

            if (batch > 0)
            {
                var taken = new List<long>();
                index.Take(new StructuralRow(((batch - 1) * 7) + 1, Utf8String.Of("r1")), 1, taken);
            }

            index.Commit();
            var (trace, files) = index.Blocks!.Value;
            Assert.All(trace.Runs.SelectMany(r => r.Blocks), b => Assert.IsType<LazyTraceBlock>(b));
            if (DbspNet.Core.Operators.Stateful.Blocks.BlockAccess.Spiller is null)
            {
                Assert.Equal(trace.Runs.Count, files); // (a mid-batch spill, §5.9, files some runs first)
            }
        }

        Assert.Equal((40 * 7) - 39, index.Count);
    });
}
