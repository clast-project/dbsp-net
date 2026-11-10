// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// In-step delta spilling, D1–D3 (docs/design-bounded-memory.md §5.14). Required:
//   1. a block-backed Z-set (D1) answers every read — Count, WeightOf/Contains, enumeration, equality —
//      exactly as the managed set of the same adds, and every derived set (sum, difference, negation,
//      filter, scale, map, in-place merge, clone) equals the managed result;
//   2. the spilling builder (D2) builds managed below the threshold and block backed above it (or
//      always, forced), consolidating across frozen chunks, cancellations included;
//   3. with the knob on, compiled circuits (D3) give the same output every tick as a twin with it off,
//      across the random-query generator, and actually build block-backed deltas;
//   4. a fan-out join past the threshold emits a block-backed delta; below it, a managed one;
//   6. a blocked Z-set trace (view, DISTINCT) adopts a block-backed delta's runs (§5.19) and reads exactly
//      as one that integrated the same deltas managed; a layout it cannot read falls back to a level;
//   7. (D4, §5.22) a block-backed indexed delta reads exactly as the managed one of the same adds —
//      group count, enumeration, GroupFor/ContainsKey, equality, sum, difference, flatten — and a blocked
//      indexed trace adopting such deltas reads exactly as one integrating them managed;
//   5. the left join's cross-product guard trips only on a blowup (output far past both groups) into a
//      managed builder: not on a big 1:N lookup, and not when the output spills (§5.16).
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Tests.EndToEnd;
using DbspNet.Tests.Persistence;
using DbspNet.Tests.Sql;
using Xunit;

namespace DbspNet.Tests.Operators.Stateful;

[Collection(nameof(BlockedCheckpointTests))] // the guard tests set a process-wide limit
public class BlockedDeltaTests
{
    private static readonly Schema RowSchema = new(
    [
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static readonly ITraceBlockFormat Format =
        TraceBlockFormats.Create(RowSchema, Schema.Empty) ?? throw new InvalidOperationException("no block format: Persistence not loaded");

    private static readonly string?[] Strings = [null, "a", "bb", new string('x', 300)];

    private static StructuralRow Row(int k, int s) => new(SqlTestHelpers.EncodeStrings([k, Strings[s]]));

    // (k, s, weight) adds: few keys, so duplicates and cancellations are common.
    private static readonly Gen<(int K, int S, long W)[]> GenAdds =
        Gen.Select(Gen.Int[0, 12], Gen.Int[0, 3], Gen.OneOfConst(-2L, -1L, 1L, 1L, 2L)).Array[0, 120];

    private static ZSet<StructuralRow, Z64> Managed((int K, int S, long W)[] adds)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        foreach (var (k, s, w) in adds)
        {
            b.Add(Row(k, s), new Z64(w));
        }

        return b.Build();
    }

    // Built through the spilling builder under threshold rows (0 = forced) freezing every chunk rows.
    private static ZSet<StructuralRow, Z64> Spilled((int K, int S, long W)[] adds, long rows, int chunk)
    {
        using var _ = SpillSettings.UseStepSpill(rows, chunk);
        var b = DeltaSpill.Rows(Format, adds.Length);
        foreach (var (k, s, w) in adds)
        {
            b.Add(Row(k, s), new Z64(w));
        }

        return b.Build();
    }

    private static void AssertSame(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected.IsEmpty, actual.IsEmpty);
        Assert.True(expected.Equals(actual), $"expected {expected} got {actual}");
        Assert.True(actual.Equals(expected));
        for (var k = 0; k <= 12; k++)
        {
            for (var s = 0; s < Strings.Length; s++)
            {
                var row = Row(k, s);
                Assert.Equal(expected.WeightOf(row), actual.WeightOf(row));
                Assert.Equal(expected.Contains(row), actual.Contains(row));
            }
        }

        var listed = actual.Select(e => e.Key + "#" + e.Value.Value).Order(StringComparer.Ordinal).ToList();
        Assert.Equal(expected.Select(e => e.Key + "#" + e.Value.Value).Order(StringComparer.Ordinal), listed);
        Assert.Equal(listed.Count, listed.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(expected.Keys.Count(), actual.Keys.Count());
        Assert.Equal(expected.SumWeights(), actual.SumWeights());
    }

    [Fact]
    public void Block_backed_sets_read_and_derive_as_managed_ones()
    {
        var multiRun = false;
        Gen.Select(GenAdds, GenAdds, Gen.Int[1, 9], Gen.Bool).Sample((a, b, chunk, forced) =>
        {
            var rows = forced ? 0L : chunk;
            var ma = Managed(a);
            var mb = Managed(b);
            var sa = Spilled(a, rows, chunk);
            var sb = Spilled(b, rows, chunk);
            AssertSame(ma, sa);

            // Forced: every non-empty set is blocks. Thresholded: blocks once a chunk froze.
            if (forced && !ma.IsEmpty)
            {
                Assert.True(sa.IsExternal);
            }

            if (!forced && a.Length < chunk)
            {
                Assert.False(sa.IsExternal);
            }

            // Multi-chunk sets keep their chunks as separate runs (§5.17: never merged).
            if (sa.External is BlockedRows br && a.Length > 2 * chunk && chunk < 4)
            {
                multiRun |= br.RunCount > 1;
            }

            using var _ = SpillSettings.UseStepSpill(rows, chunk);
            AssertSame(ma + mb, sa + sb);
            AssertSame(ma + mb, sa + mb);
            AssertSame(ma + mb, ma + sb);
            AssertSame(ma - mb, sa - sb);
            AssertSame(ma - mb, ma - sb);
            AssertSame(ma - mb, sa - mb);
            AssertSame(-ma, -sa);
            AssertSame(ma.ScalarMultiply(new Z64(3)), sa.ScalarMultiply(new Z64(3)));
            AssertSame(ma.ScalarMultiply(Z64.Zero), sa.ScalarMultiply(Z64.Zero));
            AssertSame(ma.Filter(r => (int)r[0]! % 2 == 0), sa.Filter(r => (int)r[0]! % 2 == 0));
            AssertSame(ma.MapKeys(r => Row((int)r[0]! / 3, 1)), sa.MapKeys(r => Row((int)r[0]! / 3, 1)));
            AssertSame(ma, sa.Clone());

            // In place, both ways: a block-backed delta into managed state, and (the fallback) into a
            // block-backed set, which moves into a managed map first.
            var state = ma.Clone();
            state.MergeInPlace(sb);
            AssertSame(ma + mb, state);
            var blockedState = sa.Clone();
            blockedState.MergeInPlace(mb);
            AssertSame(ma + mb, blockedState);
            AssertSame(ma, sa); // the shared store was not mutated
            var dropped = Spilled(a, rows, chunk);
            dropped.RemoveKeysBelow(6, r => (int)r[0]!);
            AssertSame(ma.Filter(r => (int)r[0]! >= 6), dropped);
        }, iter: 400);
        Assert.True(multiRun, "no case built a set of several runs");
    }

    [Fact]
    public void A_blocked_zset_trace_adopts_block_backed_deltas_and_reads_as_managed()
    {
        var adopted0 = Interlocked.Read(ref DeltaSpill.Adopted);
        Gen.Select(GenAdds.Array[1, 6], Gen.Int[1, 9]).Sample((ticks, chunk) =>
        {
            var managed = new BlockedZSetTrace(Format);
            var adopting = new BlockedZSetTrace(Format);
            var sum = ZSet<StructuralRow, Z64>.Empty;
            foreach (var tick in ticks)
            {
                var delta = Spilled(tick, 0, chunk);
                managed.Integrate(Managed(tick));
                adopting.Integrate(delta);
                sum += Managed(tick);
                AssertSame(sum, managed.Current);
                AssertSame(sum, adopting.Current);
                Assert.Equal(managed.Count, adopting.Count);
                adopting.Trace.CheckRuns();
                for (var k = 0; k <= 12; k++)
                {
                    Assert.Equal(sum.WeightOf(Row(k, 2)), adopting.WeightOf(Row(k, 2)));
                }
            }
        }, iter: 200);
        Assert.True(Interlocked.Read(ref DeltaSpill.Adopted) > adopted0, "no delta was adopted");
    }

    [Fact]
    public void A_trace_does_not_adopt_a_layout_it_cannot_read()
    {
        // A trace whose s column is NOT NULL cannot take blocks where s may be null.
        var strict = TraceBlockFormats.Create(
            new Schema([new SchemaColumn("k", new SqlIntegerType(false)), new SchemaColumn("s", new SqlVarcharType(null, false))]), Schema.Empty)!;
        Assert.False(strict.Adopts(Format));
        Assert.True(Format.Adopts(strict)); // the other way round reads fine
        var renamed = TraceBlockFormats.Create(
            new Schema([new SchemaColumn("key", new SqlIntegerType(false)), new SchemaColumn("text", new SqlVarcharType(null, true))]), Schema.Empty)!;
        Assert.True(renamed.Adopts(Format)); // names are not layout

        var trace = new BlockedZSetTrace(strict);
        var adds = new (int, int, long)[] { (1, 1, 1), (2, 2, 1), (3, 1, -1) };
        var adopted0 = Interlocked.Read(ref DeltaSpill.Adopted);
        trace.Integrate(Spilled(adds, 0, 2));
        Assert.Equal(adopted0, Interlocked.Read(ref DeltaSpill.Adopted));
        AssertSame(Managed(adds), trace.Current);
    }

    // ---- D4: indexed deltas ----
    private static readonly Schema KeySchema = new([new SchemaColumn("k", new SqlIntegerType(false))]);

    private static readonly ITraceBlockFormat IndexedFormat =
        TraceBlockFormats.Create(KeySchema, RowSchema) ?? throw new InvalidOperationException("no block format");

    private static StructuralRow Key(int k) => new(SqlTestHelpers.EncodeStrings([k]));

    // (key, value row, weight) adds over few keys and values, so groups span chunks and cancel.
    private static readonly Gen<(int K, int V, int S, long W)[]> GenIndexedAdds =
        Gen.Select(Gen.Int[0, 9], Gen.Int[0, 5], Gen.Int[0, 3], Gen.OneOfConst(-2L, -1L, 1L, 1L, 2L)).Array[0, 150];

    private static IndexedZSet<StructuralRow, StructuralRow, Z64> ManagedIndexed((int K, int V, int S, long W)[] adds)
    {
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (k, v, s, w) in adds)
        {
            b.Add(Key(k), Row(v, s), new Z64(w));
        }

        return b.Build();
    }

    private static IndexedZSet<StructuralRow, StructuralRow, Z64> SpilledIndexed((int K, int V, int S, long W)[] adds, long rows, int chunk)
    {
        using var _ = SpillSettings.UseStepSpill(rows, chunk);
        var b = DeltaSpillGroups.For<StructuralRow, StructuralRow, Z64>(IndexedFormat);
        foreach (var (k, v, s, w) in adds)
        {
            b.Add(Key(k), Row(v, s), new Z64(w));
        }

        return b.Build();
    }

    private static void AssertSameIndexed(IndexedZSet<StructuralRow, StructuralRow, Z64> expected, IndexedZSet<StructuralRow, StructuralRow, Z64> actual)
    {
        Assert.Equal(expected.GroupCount, actual.GroupCount);
        Assert.Equal(expected.IsEmpty, actual.IsEmpty);
        Assert.True(expected.Equals(actual), "managed != blocked");
        Assert.True(actual.Equals(expected), "blocked != managed");
        var seen = new HashSet<StructuralRow>();
        foreach (var (key, group) in actual)
        {
            Assert.True(seen.Add(key), $"key {key} enumerated twice");
            Assert.False(group.IsEmpty);
            Assert.True(expected.GroupFor(key).Equals(group));
        }

        Assert.Equal(expected.GroupCount, seen.Count);
        for (var k = 0; k <= 10; k++)
        {
            Assert.True(expected.GroupFor(Key(k)).Equals(actual.GroupFor(Key(k))));
            Assert.Equal(expected.ContainsKey(Key(k)), actual.ContainsKey(Key(k)));
        }
    }

    [Fact]
    public void Block_backed_indexed_sets_read_and_derive_as_managed_ones()
    {
        var builds0 = Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds);
        var multiRun = false;
        Gen.Select(GenIndexedAdds, GenIndexedAdds, Gen.Int[1, 9]).Sample((a, b, chunk) =>
        {
            var ma = ManagedIndexed(a);
            var sa = SpilledIndexed(a, 0, chunk);
            Assert.Equal(!ma.IsEmpty, sa.External is not null);
            multiRun |= sa.External is BlockedGroups { RunCount: > 1 };
            AssertSameIndexed(ma, sa);
            var mb = ManagedIndexed(b);
            var sb = SpilledIndexed(b, 0, chunk);
            AssertSameIndexed(ma + mb, sa + sb);
            AssertSameIndexed(ma - mb, sa - sb);
            AssertSameIndexed(ma + mb, sa + mb);
            Assert.True(ma.Flatten().Equals(sa.Flatten()));

            // Below the threshold the builder stays managed.
            Assert.Null(SpilledIndexed(a, 1_000, chunk).External);
        }, iter: 400);
        Assert.True(multiRun, "no case built a set of several runs");
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds) > builds0);
    }

    // §5.27: the group count reads a hash several runs hold without building its groups, unless a key
    // has both signs. Key 0's only entry is added in one chunk and cancelled in another, so its group is
    // empty; key 1 spans every chunk with one sign (a giant group); key 2 has both signs but survives.
    [Fact]
    public void A_group_count_across_runs_skips_cancelled_groups_and_counts_one_signed_ones()
    {
        var adds = new List<(int K, int V, int S, long W)> { (0, 1, 1, 1) };
        adds.AddRange(Enumerable.Range(0, 40).Select(i => (1, i % 6, i % 4, 1L)));
        adds.Add((0, 1, 1, -1));
        adds.Add((2, 2, 2, 3));
        adds.AddRange(Enumerable.Range(0, 10).Select(i => (1, i % 6, 3 - (i % 4), 2L)));
        adds.Add((2, 2, 2, -1));
        var managed = ManagedIndexed([.. adds]);
        var spilled = SpilledIndexed([.. adds], 0, 4);
        Assert.True(spilled.External is BlockedGroups { RunCount: > 1 });
        Assert.Equal(2, managed.GroupCount);
        AssertSameIndexed(managed, spilled);
    }

    [Fact]
    public void A_blocked_indexed_trace_adopts_block_backed_deltas_and_reads_as_managed()
    {
        var adopted0 = Interlocked.Read(ref DeltaSpill.Adopted);
        Gen.Select(GenIndexedAdds.Array[1, 6], Gen.Int[1, 9]).Sample((ticks, chunk) =>
        {
            var managed = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
            var adopting = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
            var sum = IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
            foreach (var tick in ticks)
            {
                managed.Integrate(ManagedIndexed(tick));
                adopting.Integrate(SpilledIndexed(tick, 0, chunk));
                sum += ManagedIndexed(tick);
                AssertSameIndexed(sum, managed.Materialize());
                AssertSameIndexed(sum, adopting.Materialize());
                adopting.CheckRuns();
            }
        }, iter: 200);
        Assert.True(Interlocked.Read(ref DeltaSpill.Adopted) > adopted0, "no indexed delta was adopted");
    }

    // ---- §5.24: merges stream a giant group by value hash ----

    // Adds where key 0 holds most entries (a giant group) over many values; other keys stay small.
    private static readonly Gen<(int K, int V, int S, long W)[]> GenSkewedAdds =
        Gen.Select(Gen.Int[0, 9], Gen.Int[0, 60], Gen.Int[0, 3], Gen.OneOfConst(-1L, 1L, 1L, 2L))
            .Select(t => t.Item1 < 7 ? (0, t.Item2, t.Item3, t.Item4) : t)
            .Array[0, 200];

    [Fact]
    public void A_giant_group_merges_streaming_and_runs_stay_in_canonical_order()
    {
        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 12;
        try
        {
            var giant0 = Interlocked.Read(ref BlockedIndexedTrace.GiantMerges);
            var fallback0 = Interlocked.Read(ref BlockedIndexedTrace.GiantFallbacks);
            Gen.Select(GenSkewedAdds.Array[1, 8], Gen.Int[1, 9], Gen.Bool).Sample((ticks, chunk, adopt) =>
            {
                var trace = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
                var sum = IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
                foreach (var tick in ticks)
                {
                    trace.Integrate(adopt ? SpilledIndexed(tick, 0, chunk) : ManagedIndexed(tick));
                    trace.Freeze();
                    sum += ManagedIndexed(tick);
                    AssertSameIndexed(sum, trace.Materialize());
                    trace.CheckRuns();
                    trace.CheckValueOrder();
                }
            }, iter: 300);
            Assert.True(Interlocked.Read(ref BlockedIndexedTrace.GiantMerges) > giant0, "no giant group was merged streaming");
            Assert.Equal(fallback0, Interlocked.Read(ref BlockedIndexedTrace.GiantFallbacks));
        }
        finally
        {
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }
    }

    // §5.35: adopting a block-backed delta of one giant key merges its runs streaming, and copies each entry
    // whole: the value-order check and the merge read stored value hashes, and a value is decoded only to sum
    // entries sharing its hash. Extended TPC-DS tick 18 lands 7.2M rows of one join key on a worker; decoding
    // and encoding them cost the left join's integrate 2.4 KB a row. The merge allocates less than one decode
    // of every value (decoding for the check as well came to 2.1 decodes, decoding for the merge alone 1.2).
    [Fact]
    public void A_giant_groups_merge_checks_value_order_without_decoding_values()
    {
        const int n = 60_000;
        var adds = Enumerable.Range(0, n).Select(i => (0, i, i % 4, 1L)).ToArray();
        var delta = SpilledIndexed(adds, 0, n / 6);
        Assert.True(delta.External is BlockedGroups);

        // One decode of every value (what the merge needs), measured on the delta itself.
        var decode = GC.GetAllocatedBytesForCurrentThread();
        long count = 0;
        foreach (var (_, group) in delta)
        {
            foreach (var _ in group)
            {
                count++;
            }
        }

        decode = GC.GetAllocatedBytesForCurrentThread() - decode;
        Assert.Equal(n, count);

        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 1000;
        var giant0 = Interlocked.Read(ref BlockedIndexedTrace.GiantMerges);
        var trace = new BlockedIndexedTrace(IndexedFormat);
        var merge = GC.GetAllocatedBytesForCurrentThread();
        try
        {
            trace.Integrate(delta);
        }
        finally
        {
            merge = GC.GetAllocatedBytesForCurrentThread() - merge;
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }

        Assert.True(Interlocked.Read(ref BlockedIndexedTrace.GiantMerges) > giant0, "the giant group was not merged streaming");
        AssertSameIndexed(ManagedIndexed(adds), trace.Materialize());
        Console.WriteLine($"GIANT decode={decode} merge={merge} ratio={(double)merge / decode:F2}");
        Assert.True(merge < decode, $"the merge allocated {merge} B, a decode of every value {decode} B");
    }

    // §5.36: a merge copies entries between blocks. A hash one run holds is copied as it is, with no row
    // decoded; entries of a hash several runs hold are decoded only to sum them, and the survivors copied.
    // Extended TPC-DS's aggregates and joins spent ~20% of a step decoding and encoding rows in trace merges.
    [Fact]
    public void A_merge_copies_entries_rather_than_decoding_and_encoding_them()
    {
        const int n = 30_000;
        var adds = Enumerable.Range(0, n).Select(i => (i, i % 50, i % 4, 1L)).ToArray();
        long Adopt(bool copy)
        {
            var delta = SpilledIndexed(adds, 0, n / 6);
            Assert.True(delta.External is BlockedGroups { RunCount: > 2 });
            var saved = BlockedIndexedTrace.CopyMerges;
            BlockedIndexedTrace.CopyMerges = copy;
            var trace = new BlockedIndexedTrace(IndexedFormat);
            var before = GC.GetAllocatedBytesForCurrentThread();
            try
            {
                trace.Integrate(delta);
            }
            finally
            {
                before = GC.GetAllocatedBytesForCurrentThread() - before;
                BlockedIndexedTrace.CopyMerges = saved;
            }

            AssertSameIndexed(ManagedIndexed(adds), trace.Materialize());
            trace.CheckRuns();
            return before;
        }

        var copied0 = Interlocked.Read(ref BlockedIndexedTrace.CopiedHashes);
        var decoding = Adopt(copy: false);
        Assert.Equal(copied0, Interlocked.Read(ref BlockedIndexedTrace.CopiedHashes));
        var copying = Adopt(copy: true);
        Assert.True(Interlocked.Read(ref BlockedIndexedTrace.CopiedHashes) - copied0 >= n, "the merge did not copy its hashes");
        Console.WriteLine($"MERGECOPY decoding={decoding} copying={copying} ratio={(double)copying / decoding:F2}");
        // 0.28 by default; 0.46 under the forced suites' 16-entry blocks and 1 MiB cache (blocks reloaded).
        Assert.True(copying * 5 < decoding * 3, $"the copying merge allocated {copying} B, the decoding one {decoding} B");
    }

    // §5.40: a hash several runs hold, as one key, is merged by stored value hash: entries copied whole,
    // values decoded only where value hashes tie. Each of 200 groups of 150 rows lies across six runs
    // (below the giant size), and the merge allocates far less than decoding every entry.
    [Fact]
    public void A_merge_sums_a_key_several_runs_hold_by_value_hash()
    {
        const int n = 30_000;
        var adds = Enumerable.Range(0, n).Select(i => (i % 200, i / 200, i % 4, 1L)).ToArray();
        long Adopt(bool byValueHash)
        {
            var delta = SpilledIndexed(adds, 0, n / 6);
            Assert.True(delta.External is BlockedGroups { RunCount: > 2 });
            var saved = BlockedIndexedTrace.ValueHashMerges;
            BlockedIndexedTrace.ValueHashMerges = byValueHash;
            var trace = new BlockedIndexedTrace(IndexedFormat);
            var before = GC.GetAllocatedBytesForCurrentThread();
            try
            {
                trace.Integrate(delta);
            }
            finally
            {
                before = GC.GetAllocatedBytesForCurrentThread() - before;
                BlockedIndexedTrace.ValueHashMerges = saved;
            }

            AssertSameIndexed(ManagedIndexed(adds), trace.Materialize());
            trace.CheckRuns();
            trace.CheckValueOrder();
            return before;
        }

        var merged0 = Interlocked.Read(ref BlockedIndexedTrace.ValueHashMerged);
        var decoding = Adopt(byValueHash: false);
        Assert.Equal(merged0, Interlocked.Read(ref BlockedIndexedTrace.ValueHashMerged));
        var streaming = Adopt(byValueHash: true);
        Assert.True(Interlocked.Read(ref BlockedIndexedTrace.ValueHashMerged) > merged0, "no hash was merged by value hash");
        Console.WriteLine($"VALUEHASHMERGE decoding={decoding} streaming={streaming} ratio={(double)streaming / decoding:F2}");
        // 0.28 by default; 0.52 under the forced suites' 16-entry blocks and 1 MiB cache (blocks reloaded).
        Assert.True(streaming * 5 < decoding * 3, $"the value-hash merge allocated {streaming} B, the decoding one {decoding} B");
    }

    // §5.40: with value-hash merges off, the same ticks give the same trace (the property test below runs
    // with them on).
    [Fact]
    public void A_merge_by_value_hash_matches_the_decoding_merge()
    {
        Gen.Select(GenIndexedAdds.Array[1, 6], Gen.Int[1, 9]).Sample((ticks, chunk) =>
        {
            var saved = BlockedIndexedTrace.ValueHashMerges;
            try
            {
                var on = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
                var off = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
                foreach (var tick in ticks)
                {
                    BlockedIndexedTrace.ValueHashMerges = true;
                    on.Integrate(SpilledIndexed(tick, 0, chunk));
                    on.Freeze();
                    BlockedIndexedTrace.ValueHashMerges = false;
                    off.Integrate(SpilledIndexed(tick, 0, chunk));
                    off.Freeze();
                    AssertSameIndexed(off.Materialize(), on.Materialize());
                    on.CheckRuns();
                    on.CheckValueOrder();
                }
            }
            finally
            {
                BlockedIndexedTrace.ValueHashMerges = saved;
            }
        }, iter: 300);
    }

    // §5.36: entries of one hash in several runs are summed before they are copied (cancelling ones
    // dropped), and the merged runs stay canonical.
    [Fact]
    public void A_copying_merge_sums_entries_several_runs_hold()
    {
        var summed0 = Interlocked.Read(ref BlockedIndexedTrace.SummedHashes) + Interlocked.Read(ref BlockedIndexedTrace.ValueHashMerged);
        Gen.Select(GenIndexedAdds.Array[1, 6], Gen.Int[1, 9]).Sample((ticks, chunk) =>
        {
            var trace = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 16, blockEntries: 8);
            var sum = IndexedZSet<StructuralRow, StructuralRow, Z64>.Empty;
            foreach (var tick in ticks)
            {
                trace.Integrate(SpilledIndexed(tick, 0, chunk));
                trace.Freeze();
                sum += ManagedIndexed(tick);
                AssertSameIndexed(sum, trace.Materialize());
                trace.CheckRuns();
                trace.CheckValueOrder();
            }
        }, iter: 300);
        Assert.True(Interlocked.Read(ref BlockedIndexedTrace.SummedHashes) + Interlocked.Read(ref BlockedIndexedTrace.ValueHashMerged) > summed0,
            "no merge summed a hash several runs held");
    }

    // §5.36: a block-backed delta hands out a giant group streamed: it reads as the managed group (rows,
    // count, lookups, sums), cancelling and duplicated entries across runs included.
    [Fact]
    public void A_streamed_giant_group_reads_as_the_managed_one()
    {
        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 12;
        var streamed0 = Interlocked.Read(ref BlockedGroups.StreamedGroups);
        try
        {
            Gen.Select(GenSkewedAdds, Gen.Int[1, 9]).Sample((adds, chunk) =>
            {
                IndexedZSet<StructuralRow, StructuralRow, Z64> spilled;
                using (TraceBlocksMode.Use(join: true, blockEntries: 4))
                {
                    spilled = SpilledIndexed(adds, 0, chunk);
                }

                var managed = ManagedIndexed(adds);
                AssertSameIndexed(managed, spilled);
                foreach (var (key, group) in spilled)
                {
                    var expected = managed.GroupFor(key);
                    Assert.Equal(expected.Count, group.Count);
                    foreach (var (value, weight) in expected)
                    {
                        Assert.Equal(weight, group.WeightOf(value));
                    }

                    Assert.True((expected + expected).Equals(group + group));
                    Assert.True(expected.Negate().Equals(group.Negate()));
                }
            }, iter: 300);
        }
        finally
        {
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }

        Assert.True(Interlocked.Read(ref BlockedGroups.StreamedGroups) > streamed0, "no giant group was streamed");
    }

    // §5.36: a giant group whose run is out of value-hash order (as an older file could hold it) is not
    // streamed: the walk's order check rejects it, and the group is resolved, summing correctly.
    [Fact]
    public void A_giant_group_out_of_value_order_is_resolved_not_streamed()
    {
        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 12;
        try
        {
            var trace = new BlockedIndexedTrace(IndexedFormat, freezeEntries: int.MaxValue, maxManaged: int.MaxValue, blockEntries: 8, compact: false);
            var key = Key(0);
            var values = Enumerable.Range(0, 40).Select(v => Row(v, 1)).OrderByDescending(v => (uint)v.GetHashCode()).ToArray();
            var n = values.Length;
            var blocks = values.Chunk(8).Select(part => IndexedFormat.Encode(
                Enumerable.Repeat(key, part.Length).ToArray(), part, Enumerable.Repeat(1L, part.Length).ToArray(),
                Enumerable.Repeat(key.GetHashCode(), part.Length).ToArray(), part.Length)).ToArray();
            trace.AdoptRun(blocks, n, null);
            var expected = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            foreach (var v in values)
            {
                expected.Add(key, v, Z64.One);
            }

            var delta = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            for (var v = 0; v < 60; v++)
            {
                var w = v < 20 ? -1L : 1L;
                delta.Add(key, Row(v, 1), new Z64(w));
                expected.Add(key, Row(v, 1), new Z64(w));
            }

            trace.Integrate(delta.Build());
            trace.Freeze();
            Assert.Equal(2, trace.Runs.Count);
            var hashes = trace.Runs.Select(_ => new[] { (uint)key.GetHashCode() }).ToArray();
            var groups = BlockedGroups.Create(trace, hashes, IndexedFormat)!;
            var streamed0 = Interlocked.Read(ref BlockedGroups.StreamedGroups);
            AssertSameIndexed(expected.Build(), new IndexedZSet<StructuralRow, StructuralRow, Z64>(groups));
            Assert.Equal(streamed0, Interlocked.Read(ref BlockedGroups.StreamedGroups));
        }
        finally
        {
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }
    }

    // §5.37: taking a giant group from a block-backed delta decodes none of its rows (only its count is
    // read, by stored value hashes): extended TPC-DS's skewed left joins each held a 7.2M-row group of
    // their left delta decoded (~1.1 KB a row), three at once. Measured as this thread's allocation (the
    // heap is shared with tests running alongside).
    [Fact]
    public void A_streamed_giant_group_decodes_none_of_its_rows_until_read()
    {
        const int n = 60_000;
        var adds = Enumerable.Range(0, n).Select(i => (0, i, i % 4, 1L)).ToArray();
        long Taken(bool stream)
        {
            var delta = SpilledIndexed(adds, 0, n / 6);
            var saved = BlockedGroups.StreamGiantGroups;
            var savedGiant = BlockedIndexedTrace.GiantGroupEntries;
            BlockedGroups.StreamGiantGroups = stream;
            BlockedIndexedTrace.GiantGroupEntries = 1000;
            try
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                ZSet<StructuralRow, Z64>? group = null;
                foreach (var (_, g) in delta)
                {
                    group = g;
                }

                var taken = GC.GetAllocatedBytesForCurrentThread() - before;
                Assert.Equal(n, group!.Count);
                Assert.Equal(stream, group.IsExternal);
                return taken;
            }
            finally
            {
                BlockedGroups.StreamGiantGroups = saved;
                BlockedIndexedTrace.GiantGroupEntries = savedGiant;
            }
        }

        var materialized = Taken(stream: false);
        var streamed = Taken(stream: true);
        Console.WriteLine($"STREAMED taken={streamed} materialized={materialized} ratio={(double)streamed / materialized:F3}");
        // ~0 by default; 0.28 under the forced suites' 1 MiB cache, where the count's walk reloads blocks.
        Assert.True(streamed * 2 < materialized, $"taking a streamed group allocated {streamed} B, a materialized one {materialized} B");
    }

    [Fact]
    public void A_giant_group_out_of_value_order_falls_back_and_merges_correctly()
    {
        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 12;
        try
        {
            var fallback0 = Interlocked.Read(ref BlockedIndexedTrace.GiantFallbacks);
            var trace = new BlockedIndexedTrace(IndexedFormat, freezeEntries: 1_000, blockEntries: 8);

            // A run (as an older file would hold it) with key 0's 40 values in DEScending value hash.
            var values = Enumerable.Range(0, 40).Select(v => Row(v, 1)).OrderByDescending(v => (uint)v.GetHashCode()).ToArray();
            var n = values.Length;
            var key = Key(0);
            var block = IndexedFormat.Encode(
                Enumerable.Repeat(key, n).ToArray(), values, Enumerable.Repeat(1L, n).ToArray(),
                Enumerable.Repeat(key.GetHashCode(), n).ToArray(), n);
            trace.AdoptRun([block], n, null);
            var expected = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            foreach (var v in values)
            {
                expected.Add(key, v, Z64.One);
            }

            // A second run retracting half and adding more, then a merge of the two.
            var delta = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            for (var v = 0; v < 60; v++)
            {
                var w = v < 20 ? -1L : 1L;
                delta.Add(key, Row(v, 1), new Z64(w));
                expected.Add(key, Row(v, 1), new Z64(w));
            }

            trace.Integrate(delta.Build());
            trace.Freeze();
            Assert.Single(trace.Runs);
            Assert.True(Interlocked.Read(ref BlockedIndexedTrace.GiantFallbacks) > fallback0, "the out-of-order run did not fall back");
            AssertSameIndexed(expected.Build(), trace.Materialize());
            trace.CheckRuns();
            trace.CheckValueOrder(); // the fallback's output is canonical
        }
        finally
        {
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }
    }

    [Fact]
    public void A_traces_count_is_streamed_and_equals_its_materialized_count()
    {
        Gen.Select(GenAdds.Array[1, 10], Gen.Bool.Array[10]).Sample((ticks, freeze) =>
        {
            var trace = new BlockedIndexedTrace(Format, freezeEntries: 16, blockEntries: 8);
            for (var t = 0; t < ticks.Length; t++)
            {
                trace.IntegrateRows(Managed(ticks[t]));
                if (freeze[t])
                {
                    trace.Freeze(); // a mix of runs and managed levels
                }

                Assert.Equal(trace.MaterializeRows().Count, trace.CountRows());
            }

            // A fresh view over the same trace counts without materializing.
            var view = new BlockedZSetTrace(Format);
            foreach (var tick in ticks)
            {
                view.Integrate(Managed(tick));
            }

            Assert.Equal(view.Trace.MaterializeRows().Count, view.Count);
        }, iter: 300);
    }

    [Fact]
    public void Builder_count_and_cancellation_across_chunks()
    {
        using var _ = SpillSettings.UseStepSpill(4, 4);
        var b = DeltaSpill.Rows(Format, 1_000_000);
        for (var k = 0; k < 10; k++)
        {
            b.Add(Row(k, 1), Z64.One);
        }

        Assert.True(b.Count >= 10); // an upper bound once chunks froze
        for (var k = 0; k < 10; k++)
        {
            b.Add(Row(k, 1), new Z64(-1));
        }

        var built = b.Build();
        Assert.True(built.IsEmpty);
        Assert.Equal(0, built.Count);
        Assert.Throws<InvalidOperationException>(() => b.Add(Row(1, 1), Z64.One));
    }

    [Fact]
    public void Knob_off_builds_the_plain_builder()
    {
        using var _ = SpillSettings.UseStepSpill(null);
        var b = DeltaSpill.Rows(Format, 0);
        for (var k = 0; k < 10_000; k++)
        {
            b.Add(Row(k, 0), Z64.One);
        }

        Assert.False(b.Build().IsExternal);
        Assert.False(DeltaSpill.For<StructuralRow, Z64>(null, 0).Build().IsExternal);
    }

    private static readonly string[] JoinDdl =
    [
        "CREATE TABLE f (k INT NOT NULL, x BIGINT NOT NULL, s VARCHAR)",
        "CREATE TABLE d (k INT NOT NULL, y BIGINT NOT NULL)",
    ];

    private const string JoinSql = "SELECT f.k, f.x + d.y AS z, f.s FROM f JOIN d ON f.k = d.k WHERE f.x >= 0";

    private static readonly CompileOptions Structural = CompileOptions.Default with { ShareArrangements = true };

    private static CompiledQuery CompileJoin(bool blocks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in JoinDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(JoinSql))).Query;
        using var mode = TraceBlocksMode.Use(join: blocks);
        return PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
    }

    // Tick 0: 20 fact rows on key 1, then a dimension tick with 30 rows on key 1 — a 600-row fan-out.
    private static List<ZSet<StructuralRow, Z64>> RunJoin(CompiledQuery q)
    {
        var outputs = new List<ZSet<StructuralRow, Z64>>();
        for (var i = 0; i < 20; i++)
        {
            q.Table("f").Insert(1, (long)i, "s" + (i % 3));
        }

        q.Step();
        outputs.Add(q.Current);
        for (var i = 0; i < 30; i++)
        {
            q.Table("d").Insert(1, (long)(100 * i));
        }

        q.Step();
        outputs.Add(q.Current);
        q.Table("f").Delete(1, 3L, "s0");
        q.Table("d").Insert(2, 7L);
        q.Step();
        outputs.Add(q.Current);
        return outputs;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_fan_out_join_past_the_threshold_emits_blocks(bool blockedTraces)
    {
        var expected = RunJoin(CompileJoin(blockedTraces));
        using var _ = SpillSettings.UseStepSpill(100, 1);
        var actual = RunJoin(CompileJoin(blockedTraces));
        for (var t = 0; t < expected.Count; t++)
        {
            AssertSameRows(expected[t], actual[t]);
        }

        Assert.Equal(600, actual[1].Count);
        Assert.True(actual[1].IsExternal, "the 600-row tick is past the threshold");
        Assert.False(actual[2].IsExternal, "a 30-row tick is below it");
    }

    // HAVING reads an aggregate the SELECT does not, so the aggregate's row is wider than plan.Schema.
    private const string AggregateSql = "SELECT k, MAX(s) AS hs, SUM(x) AS sx FROM f GROUP BY k HAVING SUM(x) > 5 AND COUNT(*) > 0";

    private static CompiledQuery CompileAggregate()
    {
        var resolver = new Resolver(new Catalog());
        foreach (var ddl in JoinDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(AggregateSql))).Query;
        using var mode = TraceBlocksMode.Use(join: false, aggregate: true);
        return PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
    }

    private static IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow> AggregateOf(CompiledQuery q) =>
        q.Circuit.Operators.OfType<IncrementalAggregateOp<StructuralRow, StructuralRow, StructuralRow>>().Single();

    // Tick 0: 300 groups; tick 1: 250 of them change (a retraction and an insertion each); tick 2: a few.
    private static List<(ZSet<StructuralRow, Z64> Output, bool Blocked)> RunAggregate(CompiledQuery q)
    {
        var outputs = new List<(ZSet<StructuralRow, Z64>, bool)>();
        void Step()
        {
            q.Step();
            outputs.Add((q.Current, AggregateOf(q).LastOutputBlocked));
        }

        for (var k = 0; k < 300; k++)
        {
            q.Table("f").Insert(k, (long)(k % 10), "s" + (k % 7));
        }

        Step();
        for (var k = 0; k < 250; k++)
        {
            q.Table("f").Insert(k, 3L, "t" + (k % 5));
        }

        Step();
        q.Table("f").Delete(1, 1L, "s1");
        q.Table("f").Insert(400, 9L, null);
        Step();
        return outputs;
    }

    [Fact]
    public void An_aggregate_past_the_threshold_emits_its_rows_as_blocks()
    {
        List<(ZSet<StructuralRow, Z64> Output, bool Blocked)> expected;
        CompiledQuery managed;
        using (SpillSettings.UseStepSpill(null))
        {
            managed = CompileAggregate();
            Assert.False(AggregateOf(managed).IsFused, "knob off: aggregate and flatten are two operators");
            expected = RunAggregate(managed);
        }

        using var _ = SpillSettings.UseStepSpill(100, 1);
        var q = CompileAggregate();
        Assert.True(AggregateOf(q).IsFused);

        // Fused, the circuit keeps the unfused shape, so a checkpoint restores across the two.
        Assert.Equal(managed.Circuit.Operators.Count, q.Circuit.Operators.Count);
        Assert.Equal(
            DbspNet.Persistence.SnapshotManifest.ComputeCircuitFingerprint(managed.Circuit),
            DbspNet.Persistence.SnapshotManifest.ComputeCircuitFingerprint(q.Circuit));
        var actual = RunAggregate(q);
        for (var t = 0; t < expected.Count; t++)
        {
            AssertSameRows(expected[t].Output, actual[t].Output);
        }

        Assert.True(actual[0].Blocked, "300 new groups are past the threshold");
        Assert.True(actual[1].Blocked, "250 changed groups emit 500 rows");
        Assert.False(actual[2].Blocked, "a few rows are below it");
    }

    // §5.36: the aggregate reads each group of a block-backed delta once, as enumerated. It walked the
    // delta's keys (which resolves every group, values and all) and then probed each key's group again.
    [Fact]
    public void An_aggregate_reads_a_block_backed_deltas_groups_once()
    {
        using var _ = SpillSettings.UseStepSpill(100, 1);
        var q = CompileAggregate();
        for (var k = 0; k < 300; k++)
        {
            q.Table("f").Insert(k, (long)(k % 10), "s" + (k % 7));
        }

        var builds0 = Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds);
        var probes0 = BlockedGroups.ThreadProbes;
        q.Step();
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds) > builds0, "the aggregate's input was not block backed");
        Assert.Equal(0, BlockedGroups.ThreadProbes - probes0);
        Assert.Equal(120, q.Current.Count); // HAVING SUM(x) > 5 keeps the groups with x in 6..9
    }

    // §5.36: a left join takes a right-only key's right group as enumerated, not by probing the right
    // delta for it again.
    [Fact]
    public void A_left_join_reads_a_block_backed_right_deltas_groups_once()
    {
        using var _ = SpillSettings.UseStepSpill(100, 1);
        var q = CompileLeftJoin();
        for (var k = 0; k < 300; k++)
        {
            q.Table("d").Insert(k, (long)k);
        }

        var builds0 = Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds);
        var probes0 = BlockedGroups.ThreadProbes;
        q.Step();
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds) > builds0, "the join's right delta was not block backed");
        Assert.Equal(0, BlockedGroups.ThreadProbes - probes0);
        Assert.Equal(0, q.Current.Count);
        q.Table("f").Insert(5, 1L, "s");
        q.Step();
        Assert.Equal(1, q.Current.Count);
    }

    // §5.36: left joins over a giant key whose left delta group is streamed give a twin's rows every tick:
    // matched, unmatched, gaining a match (the group summed with the old one), losing one, and the
    // residual form (which looks rows up in the group).
    [Theory]
    [InlineData("SELECT f.k, f.x, d.y FROM f LEFT JOIN d ON f.k = d.k")]
    [InlineData("SELECT f.k, f.x, d.y FROM f LEFT JOIN d ON f.k = d.k AND f.x > d.y")]
    public void A_left_join_over_a_streamed_giant_group_matches_its_twin(string sql)
    {
        // A parallel program, as extended TPC-DS runs: the left delta reaches the join through an exchange
        // index's gather, block backed. Worker threads start at compile and keep the settings then in force.
        ParallelCompiledProgram Compile(bool spill)
        {
            using var mode = TraceBlocksMode.Use(join: true, blockEntries: 16);
            using var _ = spill ? SpillSettings.UseStepSpill(100, 1) : SpillSettings.UseStepSpill(null);
            Assert.True(SqlProgram.TryCompileParallel(
                [.. JoinDdl, "CREATE VIEW v AS " + sql], new HashSet<string> { "v" }, 2, out var p));
            return p!;
        }

        List<ZSet<StructuralRow, Z64>> Run(ParallelCompiledProgram q)
        {
            var outputs = new List<ZSet<StructuralRow, Z64>>();
            void Step()
            {
                q.Step();
                outputs.Add(q.Outputs["v"].CurrentView);
            }

            // Tick 0: a giant key with no match (null-padded). Tick 1: it gains a match, and grows.
            // Tick 2: more rows, still matched (some cancel). Tick 3: the match goes.
            q.Table("f").Push(Enumerable.Range(0, 1500).Select(i => (new object?[] { 1, (long)i, "s" + (i % 3) }, 1L)));
            Step();
            q.Table("d").Insert(1, 700L);
            q.Table("f").Push(Enumerable.Range(1500, 1000).Select(i => (new object?[] { 1, (long)i, null }, 1L)));
            Step();
            q.Table("f").Push(Enumerable.Range(0, 1500).Where(i => i % 7 == 0).Select(i => (new object?[] { 1, (long)i, "s" + (i % 3) }, -1L))
                .Concat(Enumerable.Range(2500, 1500).Select(i => (new object?[] { 1, (long)i, "t" }, 1L))));
            Step();
            q.Table("d").Delete(1, 700L);
            q.Table("f").Push(Enumerable.Range(4000, 1000).Select(i => (new object?[] { 1, (long)i, "u" }, 1L)));
            Step();
            return outputs;
        }

        var saved = BlockedIndexedTrace.GiantGroupEntries;
        BlockedIndexedTrace.GiantGroupEntries = 200;
        var streamed0 = Interlocked.Read(ref BlockedGroups.StreamedGroups);
        try
        {
            using var plain = Compile(spill: false);
            using var spilled = Compile(spill: true);
            var expected = Run(plain);
            var actual = Run(spilled);
            for (var t = 0; t < expected.Count; t++)
            {
                AssertSameRows(expected[t], actual[t]);
            }
        }
        finally
        {
            BlockedIndexedTrace.GiantGroupEntries = saved;
        }

        Assert.True(Interlocked.Read(ref BlockedGroups.StreamedGroups) > streamed0, "no giant group was streamed");
    }

    private const string LeftJoinSql = "SELECT f.k, f.x, d.y FROM f LEFT JOIN d ON f.k = d.k";

    private static CompiledQuery CompileLeftJoin()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in JoinDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(LeftJoinSql))).Query;
        return PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
    }

    // One key with `left` fact rows and `right` dimension rows, in one tick.
    private static ZSet<StructuralRow, Z64> LeftJoinOneKey(int left, int right)
    {
        var q = CompileLeftJoin();
        for (var i = 0; i < left; i++)
        {
            q.Table("f").Insert(1, (long)i, "s");
        }

        for (var i = 0; i < right; i++)
        {
            q.Table("d").Insert(1, (long)i);
        }

        q.Step();
        return q.Current;
    }

    [Fact]
    public void Cross_product_guard_trips_only_on_a_blowup_into_a_managed_builder()
    {
        var saved = IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows;
        IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows = 100;
        try
        {
            // Managed output for the first two cases, whatever the environment (a forced suite run spills).
            using var managed = SpillSettings.UseStepSpill(null);

            // A 1:N lookup past the limit: no more output than input.
            Assert.Equal(300, LeftJoinOneKey(300, 1).Count);

            // A real blowup (400 rows from 40) trips it ...
            var ex = Assert.Throws<InvalidOperationException>(() => LeftJoinOneKey(20, 20));
            Assert.Contains("oversized per-key cross product", ex.Message, StringComparison.Ordinal);

            // ... unless the output spills.
            ZSet<StructuralRow, Z64> spilled;
            using (SpillSettings.UseStepSpill(50, 1))
            {
                spilled = LeftJoinOneKey(20, 20);
            }

            Assert.Equal(400, spilled.Count);
            Assert.True(spilled.IsExternal);
        }
        finally
        {
            IncrementalLeftJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>.CrossProductGuardRows = saved;
        }
    }

    private static void AssertSameRows(ZSet<StructuralRow, Z64> expected, ZSet<StructuralRow, Z64> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        Assert.True(expected.Equals(actual), $"expected {expected} got {actual}");
    }

    [Fact]
    public void Random_queries_match_a_twin_every_tick_with_every_delta_forced_to_blocks()
    {
        var blocked0 = Interlocked.Read(ref DeltaSpill.BlockedBuilds);
        var adopted0 = Interlocked.Read(ref DeltaSpill.Adopted);
        var groups0 = Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds);
        Gen.Select(RandomQuery.GenQuery, RandomQuery.GenTicks, Gen.Int[1, 3], Gen.Bool)
            .Sample((sql, ticks, chunk, blocks) => Twin(sql, ticks, chunk, blocks), iter: 600);
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedBuilds) > blocked0, "no delta was built as blocks");
        Assert.True(Interlocked.Read(ref DeltaSpill.Adopted) > adopted0, "no blocked DISTINCT adopted a delta");
        Assert.True(Interlocked.Read(ref DeltaSpill.BlockedGroupBuilds) > groups0, "no indexed delta was built as blocks");
    }

    private static bool Twin(string sql, IReadOnlyList<IReadOnlyList<InputEvent>> ticks, int chunk, bool blocks)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var ddl in RandomQuery.FixedDdl)
        {
            resolver.Resolve(Parser.ParseStatement(ddl));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(sql))).Query;
        CompiledQuery plain, spilled;
        using (TraceBlocksMode.Use(join: blocks))
        {
            plain = PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
            using var _ = SpillSettings.UseStepSpill(0, chunk);
            spilled = PlanToCircuit.Compile(plan, Structural, StructuralRowCodec.Instance);
        }

        using var run = SpillSettings.UseStepSpill(0, chunk);
        foreach (var tick in ticks)
        {
            foreach (var q in new[] { plain, spilled })
            {
                foreach (var ev in tick.Where(e => q.Inputs.ContainsKey(e.Table)))
                {
                    q.Table(ev.Table).Push([(ev.Row, ev.Weight)]);
                }

                q.Step();
            }

            if (!plain.Current.Equals(spilled.Current))
            {
                Console.Error.WriteLine($"SQL: {sql}\nplain:   {plain.Current}\nspilled: {spilled.Current}");
                return false;
            }
        }

        return true;
    }

    // §5.27: an exchange keeps a block-backed delta as blocks. Every row of two block-backed shards is
    // shuffled to worker 0 (the skew a low-cardinality key makes), past the step threshold, so the
    // gather must spill instead of collecting all of them managed; worker 0's output is the union.
    [Theory]
    [InlineData(4)]
    [InlineData(12)]
    public void An_exchange_keeps_a_block_backed_delta_as_blocks(int workers)
    {
        var a = Enumerable.Range(0, 300).Select(i => (i, i % 4, 1L)).ToArray();
        var b = Enumerable.Range(300, 300).Select(i => (i, i % 4, 2L)).ToArray();
        using var _ = SpillSettings.UseStepSpill(100, 10);
        using var pc = DbspNet.Core.Circuit.ParallelCircuit.Build(workers, cb =>
        {
            var (_, input) = cb.Input<ZSet<StructuralRow, Z64>>(ZSet.Empty<StructuralRow, Z64>(), (x, y) => x.Plus(y), name: "in");
            input.DeltaFormat = Format;
            cb.Output(cb.Exchange(input, static _ => 0), name: "out");
        });
        var sa = Spilled(a, 100, 10);
        var sb = Spilled(b, 100, 10);
        Assert.True(sa.IsExternal && sb.IsExternal);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 0).Push(sa);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", workers - 1).Push(sb);

        pc.Step();

        var out0 = pc.WorkerOutput<ZSet<StructuralRow, Z64>>("out", 0).Current;
        Assert.True(out0.IsExternal, "600 rows on one worker are past the 100-row threshold");
        AssertSame(Managed(a) + Managed(b), out0);
        for (var w = 1; w < workers; w++)
        {
            Assert.True(pc.WorkerOutput<ZSet<StructuralRow, Z64>>("out", w).Current.IsEmpty);
        }
    }

    // §5.35: the gather sums its spilled buckets as the union of their runs instead of decoding every row
    // into a builder and encoding it again (a skewed exchange lands 7.2M rows of extended TPC-DS tick 18 on
    // each of three workers: 13 GiB each). Two block-backed shards overlap (weights sum, some cancel) and a
    // third, managed shard joins them; everything goes to worker 0, which must hold their sum, block backed.
    [Theory]
    [InlineData(4)]
    [InlineData(12)]
    public void An_exchange_gathers_spilled_buckets_without_decoding_them(int workers)
    {
        var a = Enumerable.Range(0, 300).Select(i => (i, i % 4, 1L)).ToArray();
        var b = Enumerable.Range(200, 300).Select(i => (i, i % 4, i < 250 ? -1L : 2L)).ToArray(); // 200..249 cancel
        var c = Enumerable.Range(1000, 5).Select(i => (i, 0, 1L)).ToArray();
        using var _ = SpillSettings.UseStepSpill(100, 10);
        using var pc = DbspNet.Core.Circuit.ParallelCircuit.Build(workers, cb =>
        {
            var (_, input) = cb.Input<ZSet<StructuralRow, Z64>>(ZSet.Empty<StructuralRow, Z64>(), (x, y) => x.Plus(y), name: "in");
            input.DeltaFormat = Format;
            cb.Output(cb.Exchange(input, static _ => 0), name: "out");
        });
        var sa = Spilled(a, 100, 10);
        var sb = Spilled(b, 100, 10);
        Assert.True(sa.IsExternal && sb.IsExternal);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 0).Push(sa);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 1).Push(Managed(c));
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", workers - 1).Push(sb);
        var unioned0 = Interlocked.Read(ref DeltaSpill.Unioned);

        pc.Step();

        Assert.True(Interlocked.Read(ref DeltaSpill.Unioned) >= unioned0 + 2, "the spilled buckets were decoded");
        var out0 = pc.WorkerOutput<ZSet<StructuralRow, Z64>>("out", 0).Current;
        Assert.True(out0.External is BlockedRows, "the gather is block backed");
        AssertSame(Managed([.. a, .. b, .. c]), out0);
        Assert.Equal(200 + 250 + 5, out0.Count); // 0..199, 250..499 (200..249 cancelled), c
        for (var w = 1; w < workers; w++)
        {
            Assert.True(pc.WorkerOutput<ZSet<StructuralRow, Z64>>("out", w).Current.IsEmpty);
        }
    }

    // §5.32: the fused exchange-and-index keeps a block-backed delta as blocks too. At W=1 the same plan is a
    // GroupProject, which builds its index through D4's spilling builder; at W>1 the exchange gathered every
    // row into a managed index. Both shards' rows go to worker 0 (a low-cardinality key's skew), past the
    // threshold: the output must be block-backed and equal the managed index of the union.
    // With a pre-step (ExchangeIndexStep, the W>1 StepGroupProject) the gather is the same.
    [Theory]
    [InlineData(4, false)]
    [InlineData(12, false)]
    [InlineData(4, true)]
    [InlineData(12, true)]
    public void An_exchange_index_keeps_a_block_backed_delta_as_blocks(int workers, bool step)
    {
        var a = Enumerable.Range(0, 300).Select(i => (i, i % 4, 1L)).ToArray();
        var b = Enumerable.Range(150, 300).Select(i => (i, i % 4, 2L)).ToArray(); // overlaps a: weights sum
        using var _ = SpillSettings.UseStepSpill(100, 10);
        using var pc = DbspNet.Core.Circuit.ParallelCircuit.Build(workers, cb =>
        {
            var (_, input) = cb.Input<ZSet<StructuralRow, Z64>>(ZSet.Empty<StructuralRow, Z64>(), (x, y) => x.Plus(y), name: "in");
            input.DeltaFormat = Format;
            var indexed = step
                ? cb.ExchangeIndexStep<StructuralRow, StructuralRow, StructuralRow, Z64>(input, static row => (true, row), static _ => 0, static row => Key(KeyOf(row)))
                : cb.ExchangeIndex(input, static _ => 0, static row => Key(KeyOf(row)));
            indexed.DeltaFormat = IndexedFormat;
            cb.Output(indexed, name: "out");
        });
        var sa = Spilled(a, 100, 10);
        var sb = Spilled(b, 100, 10);
        Assert.True(sa.IsExternal && sb.IsExternal);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 0).Push(sa);
        pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", workers - 1).Push(sb);

        pc.Step();

        var expected = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (k, s, w) in a.Concat(b))
        {
            expected.Add(Key(k % 10), Row(k, s), new Z64(w));
        }

        var out0 = pc.WorkerOutput<IndexedZSet<StructuralRow, StructuralRow, Z64>>("out", 0).Current;
        Assert.True(out0.External is BlockedGroups, "600 rows on one worker are past the 100-row threshold");
        AssertSameIndexed(expected.Build(), out0);
        for (var w = 1; w < workers; w++)
        {
            Assert.True(pc.WorkerOutput<IndexedZSet<StructuralRow, StructuralRow, Z64>>("out", w).Current.IsEmpty);
        }
    }

    // §5.41: the exchange-index split keys and groups a block-backed shard's rows, and the gather unions the
    // grouped runs instead of decoding every row again to key it. Two block-backed shards overlap (weights
    // sum, some cancel), a third managed shard joins them, and the keys spread over every worker: each
    // worker's output is the managed index of its share, with grouped splits on and off.
    [Theory]
    [InlineData(4, true)]
    [InlineData(12, true)]
    [InlineData(4, false)]
    public void An_exchange_index_gathers_grouped_buckets_without_decoding_them(int workers, bool grouped)
    {
        var a = Enumerable.Range(0, 300).Select(i => (i, i % 4, 1L)).ToArray();
        var b = Enumerable.Range(200, 300).Select(i => (i, i % 4, i < 250 ? -1L : 2L)).ToArray(); // 200..249 cancel
        var c = Enumerable.Range(1000, 25).Select(i => (i, 0, 1L)).ToArray();
        var saved = DbspNet.Core.Circuit.Operators.ExchangeBuckets.GroupedSplits;
        DbspNet.Core.Circuit.Operators.ExchangeBuckets.GroupedSplits = grouped;
        try
        {
            using var _ = SpillSettings.UseStepSpill(40, 10);
            using var pc = DbspNet.Core.Circuit.ParallelCircuit.Build(workers, cb =>
            {
                var (_, input) = cb.Input<ZSet<StructuralRow, Z64>>(ZSet.Empty<StructuralRow, Z64>(), (x, y) => x.Plus(y), name: "in");
                input.DeltaFormat = Format;
                var indexed = cb.ExchangeIndex(input, static row => KeyOf(row), static row => Key(KeyOf(row)));
                indexed.DeltaFormat = IndexedFormat;
                cb.Output(indexed, name: "out");
            });
            var sa = Spilled(a, 40, 10);
            var sb = Spilled(b, 40, 10);
            Assert.True(sa.IsExternal && sb.IsExternal);
            pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 0).Push(sa);
            pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", 1).Push(Managed(c));
            pc.WorkerInput<ZSet<StructuralRow, Z64>>("in", workers - 1).Push(sb);
            var unioned0 = Interlocked.Read(ref DeltaSpill.Unioned);

            pc.Step();

            if (grouped)
            {
                Assert.True(Interlocked.Read(ref DeltaSpill.Unioned) > unioned0, "no gather unioned grouped buckets");
            }

            for (var w = 0; w < workers; w++)
            {
                var expected = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                foreach (var (k, s, wt) in a.Concat(b).Concat(c))
                {
                    if (k % 10 % workers == w)
                    {
                        expected.Add(Key(k % 10), Row(k, s), new Z64(wt));
                    }
                }

                var built = expected.Build();
                var outW = pc.WorkerOutput<IndexedZSet<StructuralRow, StructuralRow, Z64>>("out", w).Current;
                AssertSameIndexed(built, outW);
                Assert.Equal(built.GroupCount, outW.GroupCount);
            }
        }
        finally
        {
            DbspNet.Core.Circuit.Operators.ExchangeBuckets.GroupedSplits = saved;
        }
    }

    // The test rows' first column, mod 10 (the index key: ten groups over every chunk).
    private static int KeyOf(StructuralRow row) => (int)row[0]! % 10;

    // §5.32: an exchange's split fills one builder per destination side by side; each freezes at its share of
    // the step chunk, so the split holds one chunk managed, not one per destination (W x W chunks across a
    // circuit). 400 rows spread evenly over 4 destinations under a 120-row threshold: every bucket (100 rows)
    // is past its 30-row share, so every bucket is block backed, and together they are the input.
    [Fact]
    public void An_exchange_split_shares_one_step_chunk_across_its_destinations()
    {
        var adds = Enumerable.Range(0, 400).Select(i => (i, i % 4, 1L)).ToArray();
        using var _ = SpillSettings.UseStepSpill(120, 10);
        var input = Spilled(adds, 120, 10);
        Assert.True(input.IsExternal);
        var buckets = new IReadOnlyList<KeyValuePair<StructuralRow, Z64>>?[4];

        var rows = DbspNet.Core.Circuit.Operators.ExchangeBuckets.SplitSpilling(input, buckets, static row => (int)row[0]! % 4);

        Assert.Equal(400, rows);
        var sum = ZSet.Empty<StructuralRow, Z64>();
        foreach (var bucket in buckets)
        {
            var set = Assert.IsType<DbspNet.Core.Circuit.Operators.ExchangeBuckets.SetBucket<StructuralRow, Z64>>(bucket).Set;
            Assert.Equal(100, set.Count);
            Assert.True(set.IsExternal, "a 100-row bucket is past its share (120 / 4) of the step chunk");
            sum = sum.Plus(set);
        }

        AssertSame(Managed(adds), sum);
    }

    // §5.33: a stored view's handle reads the tick's delta after the step, so the integrate op kept its input's
    // value to the end of the tick: a managed set of the view's whole tick of output (~20 GB across extended
    // TPC-DS tick 18). Past the step threshold the op re-stores it as blocks the view adopts; below it, and
    // with the knob off, the delta is the managed set it was. Either way it reads as the delta, and the view
    // as the sum.
    [Fact]
    public void A_stored_views_kept_delta_is_block_backed_past_the_threshold()
    {
        DbspNet.Core.Circuit.InputHandle<ZSet<StructuralRow, Z64>>? input = null;
        DbspNet.Core.Circuit.IntegratedViewHandle<StructuralRow>? view = null;
        using var _ = SpillSettings.UseStepSpill(100, 10);
        var circuit = DbspNet.Core.Circuit.RootCircuit.Build(b =>
        {
            var (h, s) = DbspNet.Core.Operators.Linear.LinearOperators.ZSetInput<StructuralRow, Z64>(b);
            input = h;
            view = b.BlockedIntegrate(s, Format, null).View;
        });
        var big = Enumerable.Range(0, 300).Select(i => (i, i % 4, 1L)).ToArray();
        var small = Enumerable.Range(0, 50).Select(i => (i, i % 4, -1L)).ToArray();

        input!.Push(Managed(big));
        circuit.Step();
        Assert.True(view!.LastDelta.IsExternal, "300 rows are past the 100-row threshold");
        AssertSame(Managed(big), view.LastDelta);
        AssertSame(Managed(big), view.Current);

        input.Push(Managed(small));
        circuit.Step();
        Assert.False(view.LastDelta.IsExternal, "50 rows are below it");
        AssertSame(Managed(small), view.LastDelta);
        AssertSame(Managed(big) + Managed(small), view.Current);

        using (SpillSettings.UseStepSpill(null))
        {
            input.Push(Managed(big));
            circuit.Step();
            Assert.False(view.LastDelta.IsExternal, "the knob is off");
            AssertSame(Managed(big), view.LastDelta);
        }

        circuit.Step();
        Assert.True(view.LastDelta.IsEmpty);
    }

    // §5.33: the floor is the Critical chunk, under the step chunk, so a kept delta between the two must be
    // frozen whole: one under the step chunk never freezes on its own, and the re-store built a managed copy
    // (xt96/xt97: every kept delta stayed managed, and twice the memory).
    [Fact]
    public void A_kept_delta_under_one_step_chunk_is_still_block_backed()
    {
        DbspNet.Core.Circuit.InputHandle<ZSet<StructuralRow, Z64>>? input = null;
        DbspNet.Core.Circuit.IntegratedViewHandle<StructuralRow>? view = null;
        using var _ = SpillSettings.UseStepSpill(1_000_000);
        var circuit = DbspNet.Core.Circuit.RootCircuit.Build(b =>
        {
            var (h, s) = DbspNet.Core.Operators.Linear.LinearOperators.ZSetInput<StructuralRow, Z64>(b);
            input = h;
            view = b.BlockedIntegrate(s, Format, null).View;
        });
        var adds = Enumerable.Range(0, MemoryPressure.CriticalStepRows + 10).Select(i => (i, i % 4, 1L)).ToArray();

        input!.Push(Managed(adds));
        circuit.Step();

        Assert.True(view!.LastDelta.IsExternal, "past the Critical chunk, under the step chunk");
        AssertSame(Managed(adds), view.LastDelta);
        AssertSame(Managed(adds), view.Current);
    }

    // §5.34: a parallel view's row count is the number of rows whose weight summed over the shards is non-zero.
    // It was counted by forming the whole view (the end of extended TPC-DS batch 1 ran out of memory there);
    // it is now one hash-ordered merge of every shard's runs. A row on two shards counts once, a row that
    // cancels across shards not at all, and rows not yet frozen (a managed level) count too.
    [Fact]
    public void A_count_across_shards_sums_each_rows_weight_without_forming_the_view()
    {
        var a = new BlockedZSetTrace(Format);
        var b = new BlockedZSetTrace(Format);
        var c = new BlockedZSetTrace(Format);
        var aRows = Enumerable.Range(0, 100).Select(i => (i, i % 4, 1L)).ToArray();
        var bRows = Enumerable.Range(50, 100).Select(i => (i, i % 4, 1L)).Append((0, 0, -1L)).ToArray();
        var bPending = Enumerable.Range(200, 10).Select(i => (i, i % 4, 1L)).ToArray();
        var cPending = Enumerable.Range(140, 20).Select(i => (i, i % 4, -1L)).Append((205, 1, -1L)).ToArray();
        a.Integrate(Managed(aRows));
        a.Idle();
        b.Integrate(Managed(bRows));
        b.Idle();
        b.Integrate(Managed(bPending));
        c.Integrate(Managed(cPending));

        var count = BlockedIndexedTrace.CountRowsAcross([a.Trace, b.Trace, c.Trace]);

        var sum = Managed(aRows) + Managed(bRows) + Managed(bPending) + Managed(cPending);
        Assert.Equal(sum.Count, count);
        Assert.NotEqual(a.Count + b.Count + c.Count, count);
    }

    // ---- §5.37: the step chunk's byte cap ----
    private static StructuralRow WideRow(int k) => new(SqlTestHelpers.EncodeStrings([k, new string((char)('a' + (k % 26)), 2000)]));

    [Fact]
    public void A_byte_cap_freezes_wide_rows_before_the_row_cap_and_leaves_narrow_ones_managed()
    {
        // 2,000 rows under a 10,000-row cap: never frozen by rows. At 1 MiB, ~2 KB rows freeze every ~500.
        var wide = new ZSetBuilder<StructuralRow, Z64>(new SpillingRows(Format, 10_000, alwaysBlocked: false, capacity: 0, chunkBytes: 1 << 20));
        var narrow = new ZSetBuilder<StructuralRow, Z64>(new SpillingRows(Format, 10_000, alwaysBlocked: false, capacity: 0, chunkBytes: 1 << 20));
        var wideTwin = new ZSetBuilder<StructuralRow, Z64>();
        var narrowTwin = new ZSetBuilder<StructuralRow, Z64>();
        for (var i = 0; i < 2000; i++)
        {
            wide.Add(WideRow(i), new Z64(1));
            wideTwin.Add(WideRow(i), new Z64(1));
            narrow.Add(Row(i, i % 3), new Z64(1));
            narrowTwin.Add(Row(i, i % 3), new Z64(1));
        }

        var w = wide.Build();
        var n = narrow.Build();
        var parts = Assert.IsType<BlockedRows>(w.External);
        Assert.InRange(parts.RunCount, 3, 8); // ~2,048 B a row estimated: a chunk of ~512 rows
        Assert.Null(n.External);
        AssertSameRows(wideTwin.Build(), w);
        AssertSameRows(narrowTwin.Build(), n);
    }

    [Fact]
    public void A_byte_cap_never_freezes_past_the_row_cap()
    {
        var b = new ZSetBuilder<StructuralRow, Z64>(new SpillingRows(Format, 100, alwaysBlocked: false, capacity: 0, chunkBytes: 1L << 40));
        for (var i = 0; i < 1000; i++)
        {
            b.Add(Row(i, 0), new Z64(1));
        }

        Assert.Equal(10, Assert.IsType<BlockedRows>(b.Build().External).RunCount);
    }

    [Fact]
    public void An_indexed_builder_freezes_wide_groups_by_the_byte_cap()
    {
        var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>(
            new SpillingGroups(IndexedFormat, 10_000, alwaysBlocked: false, chunkBytes: 1 << 20));
        var twin = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        for (var i = 0; i < 2000; i++)
        {
            b.Add(Key(i % 7), WideRow(i), new Z64(1));
            twin.Add(Key(i % 7), WideRow(i), new Z64(1));
        }

        var built = b.Build();
        Assert.IsType<BlockedGroups>(built.External);
        AssertSameIndexed(twin.Build(), built);
    }

    [Theory]
    [InlineData(null, 0L)]
    [InlineData("", 0L)]
    [InlineData("off", 0L)]
    [InlineData("0", 0L)]
    [InlineData("256", 256L << 20)]
    public void The_step_byte_cap_setting_parses(string? setting, long bytes) =>
        Assert.Equal(bytes, SpillSettings.ByteCap(setting));
}
