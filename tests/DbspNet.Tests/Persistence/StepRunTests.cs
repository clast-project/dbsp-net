// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// One record run per step (docs/design-bounded-memory.md §5.53): pieces of ascending, disjoint hash
// ranges appended to a trace's step run become ONE run when published. Required:
//   1. a published step run holds exactly the pieces' entries (consolidated, hash-ordered), and the
//      pieces are invisible until it is published;
//   2. onto an empty trace the step is exactly one run, whatever the piece count (no merges);
//   3. a piece whose hashes do not follow the pending run starts another run, still correct;
//   4. past the spiller's threshold the step run streams to ONE file, held by the trace's lease;
//   5. Clear abandons a pending step run and leaves nothing behind.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(BlockedCheckpointTests))] // sets the process-wide cache budget and spiller
public class StepRunTests
{
    private static readonly Schema KeySchema = new([
        new SchemaColumn("k", new SqlBigintType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static readonly Schema ValueSchema = new([
        new SchemaColumn("i", new SqlIntegerType(true)),
    ]);

    private static BlockedIndexedTrace NewTrace(bool collide) =>
        new(BlockSchema.TryCreate(KeySchema, ValueSchema)!, blockEntries: 8, hash: collide ? Collide : null);

    // Few distinct hashes: pieces of one hash range hold several keys per hash.
    private static int Collide(StructuralRow k) => (int)((uint)k.GetHashCode() & 0xF000_0007u);

    private static List<(StructuralRow Key, StructuralRow Value, long Weight)> Entries(Random rnd, int keys, int from)
    {
        var e = new List<(StructuralRow, StructuralRow, long)>();
        for (var k = from; k < from + keys; k++)
        {
            var key = new StructuralRow((long)k, Utf8String.Of("p" + (k % 7)));
            for (var n = rnd.Next(1, 4); n > 0; n--)
            {
                e.Add((key, new StructuralRow(rnd.Next(3)), rnd.Next(4) == 0 ? -1 : rnd.Next(1, 3)));
            }
        }

        return e;
    }

    private static Dictionary<(StructuralRow, StructuralRow), long> Net(IEnumerable<(StructuralRow Key, StructuralRow Value, long Weight)> entries)
    {
        var net = new Dictionary<(StructuralRow, StructuralRow), long>();
        foreach (var (k, v, w) in entries)
        {
            net[(k, v)] = net.GetValueOrDefault((k, v)) + w;
        }

        foreach (var zero in net.Where(e => e.Value == 0).Select(e => e.Key).ToList())
        {
            net.Remove(zero);
        }

        return net;
    }

    private static Dictionary<(StructuralRow, StructuralRow), long> Held(BlockedIndexedTrace t)
    {
        var net = new Dictionary<(StructuralRow, StructuralRow), long>();
        foreach (var (key, group) in t.Materialize())
        {
            foreach (var (value, weight) in group)
            {
                net[(key, value)] = weight.Value;
            }
        }

        return net;
    }

    // The entries cut into `slices` pieces by the trace's own hash range, appended in ascending order.
    private static void AppendSliced(BlockedIndexedTrace t, List<(StructuralRow Key, StructuralRow Value, long Weight)> entries, int slices, bool collide, bool reverse = false)
    {
        var pieces = Enumerable.Range(0, slices).Select(_ => new List<(StructuralRow Key, StructuralRow Value, long Weight)>()).ToArray();
        foreach (var e in entries)
        {
            pieces[PartitionSlicing.Range(collide ? Collide(e.Key) : e.Key.GetHashCode(), slices)].Add(e);
        }

        foreach (var piece in reverse ? pieces.Reverse() : pieces)
        {
            t.AppendStepRun(piece.Count, i => piece[i].Key, i => piece[i].Value, i => piece[i].Weight);
        }
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 12)]
    [InlineData(true, 12)]
    [InlineData(false, 200)]
    public void Ascending_pieces_publish_as_one_run_onto_an_empty_trace(bool collide, int slices)
    {
        var rnd = new Random(slices);
        var t = NewTrace(collide);
        var entries = Entries(rnd, 3000, 0);
        AppendSliced(t, entries, slices, collide);
        Assert.True(t.StepRunPending);
        Assert.True(t.IsEmpty); // nothing visible before the publish
        t.PublishStepRun();
        Assert.False(t.StepRunPending);
        Assert.Single(t.Runs);
        Assert.Equal(1, t.StepRunsPublished);
        Assert.Equal(0, t.StepRunBreaks);
        t.CheckRuns();
        Assert.Equal(Net(entries), Held(t));
    }

    [Fact]
    public void A_step_run_onto_existing_state_sums_with_it()
    {
        var rnd = new Random(7);
        var t = NewTrace(collide: false);
        var old = Entries(rnd, 2000, 0);
        t.IntegrateEntries(old.Count, i => old[i].Key, i => old[i].Value, i => old[i].Weight);
        var before = Held(t);

        // New keys, plus (as a fallback-safe check) entries that cancel some old ones.
        var step = Entries(rnd, 2000, 5000);
        step.AddRange(old.Where((_, i) => i % 5 == 0).Select(e => (e.Key, e.Value, -e.Weight)));
        AppendSliced(t, step, 16, collide: false);
        Assert.Equal(before, Held(t));
        t.PublishStepRun();
        t.CheckRuns();
        Assert.Equal(Net(old.Concat(step)), Held(t));
        Assert.Equal(0, t.StepRunBreaks);
    }

    [Fact]
    public void A_piece_out_of_hash_order_starts_another_run()
    {
        var rnd = new Random(3);
        var t = NewTrace(collide: false);
        var entries = Entries(rnd, 3000, 0);
        AppendSliced(t, entries, 4, collide: false, reverse: true);
        t.PublishStepRun();
        Assert.Equal(3, t.StepRunBreaks);
        Assert.Equal(4, t.StepRunsPublished);
        t.CheckRuns();
        Assert.Equal(Net(entries), Held(t));
    }

    [Fact]
    public void Clear_abandons_a_pending_step_run()
    {
        var rnd = new Random(5);
        var t = NewTrace(collide: false);
        var entries = Entries(rnd, 500, 0);
        AppendSliced(t, entries, 3, collide: false);
        t.Clear();
        Assert.False(t.StepRunPending);
        t.PublishStepRun();
        Assert.True(t.IsEmpty);
        Assert.Equal(0, t.StepRunsPublished);
    }

    [Fact]
    public void Past_the_threshold_a_step_run_streams_to_one_file()
    {
        var savedBudget = BlockCache.Budget;
        var savedSpiller = BlockAccess.Spiller;
        var dir = Path.Combine(Path.GetTempPath(), "dbspnet-steprun-test-" + Guid.NewGuid().ToString("N"));
        BlockCache.Budget = 1; // every admitted body is evicted at once
        BlockAccess.Spiller = new RunSpiller(dir, 0);
        try
        {
            var rnd = new Random(9);
            var t = NewTrace(collide: false);
            var entries = Entries(rnd, 4000, 0);
            AppendSliced(t, entries, 12, collide: false);
            t.PublishStepRun();
            var run = Assert.Single(t.Runs);
            Assert.NotNull(run.Persisted);
            Assert.NotNull(run.Lease);
            Assert.Single(Directory.GetFiles(dir, "*.run"));
            t.CheckRuns();
            Assert.Equal(Net(entries), Held(t));

            // The file goes with the run.
            t.Clear();
            Assert.Empty(Directory.GetFiles(dir, "*.run"));
        }
        finally
        {
            BlockAccess.Spiller = savedSpiller;
            BlockCache.Budget = savedBudget;
            BlockCache.Clear();
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
    }
}
