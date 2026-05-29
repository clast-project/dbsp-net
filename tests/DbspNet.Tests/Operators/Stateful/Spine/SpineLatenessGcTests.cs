// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using System.Text.Json;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Phase-1 frontier-driven GC on the spine-backed
/// <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}"/>. Same properties
/// as the flat <c>LatenessGcTests</c> (bounded state, GC invisible to output,
/// inert without a frontier), plus a tick-for-tick cross-check that the spine
/// GC path agrees with the flat GC path.
/// </summary>
public class SpineLatenessGcTests
{
    private sealed record Event(long Time, long Value);

    private static ZSet<Event, Z64> Delta(params (long Time, long Value, long Weight)[] rows) =>
        ZSet.FromEntries(rows.Select(r => (new Event(r.Time, r.Value), new Z64(r.Weight))));

    private static (RootCircuit Circuit, InputHandle<ZSet<Event, Z64>> Input,
        OutputHandle<ZSet<(long Key, long Value), Z64>> Output,
        SpineIncrementalAggregateOp<long, long, long> Op)
        BuildSpine(MutableFrontier? frontier)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = frontier is null
                ? b.SpineIncrementalAggregate(grouped, new SumAggregator<long>())
                : b.SpineIncrementalAggregate(
                    grouped, new SumAggregator<long>(), frontier: frontier, monotoneKey: k => k);
            oh = b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();
        return (circuit, ih!, oh!, op);
    }

    private static (RootCircuit Circuit, InputHandle<ZSet<Event, Z64>> Input,
        OutputHandle<ZSet<(long Key, long Value), Z64>> Output,
        IncrementalAggregateOp<long, long, long> Op)
        BuildFlat(MutableFrontier frontier)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.IncrementalAggregate(
                grouped, new SumAggregator<long>(), snapshotCodec: null,
                frontier: frontier, monotoneKey: k => k);
            oh = b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<IncrementalAggregateOp<long, long, long>>().Single();
        return (circuit, ih!, oh!, op);
    }

    [Fact]
    public void SpineGc_KeepsRetainedGroupsBounded_OnMonotoneStream()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        var s = BuildSpine(frontier);

        const long Last = 200;
        for (long t = 0; t <= Last; t++)
        {
            s.Input.Push(Delta((t, 1, 1)));
            frontier.AdvanceTo(t - Lateness);
            s.Circuit.Step();
        }

        Assert.Equal((int)Lateness + 1, s.Op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_OutputMatchesFlatGc_IncludingInWindowUpdate()
    {
        const long Lateness = 10;
        var spineFrontier = new MutableFrontier();
        var flatFrontier = new MutableFrontier();
        var spine = BuildSpine(spineFrontier);
        var flat = BuildFlat(flatFrontier);
        long maxSeen = long.MinValue;

        void Tick(params (long Time, long Value, long Weight)[] rows)
        {
            spine.Input.Push(Delta(rows));
            flat.Input.Push(Delta(rows));
            foreach (var r in rows)
            {
                maxSeen = System.Math.Max(maxSeen, r.Time);
            }

            spineFrontier.AdvanceTo(maxSeen - Lateness);
            flatFrontier.AdvanceTo(maxSeen - Lateness);
            spine.Circuit.Step();
            flat.Circuit.Step();

            // The spine GC path must agree with the flat GC path tick for tick.
            Assert.Equal(flat.Output.Current, spine.Output.Current);
        }

        for (long t = 0; t <= 20; t++)
        {
            Tick((t, 1, 1));
        }

        // In-window update (frontier = 10, key 18 ≥ 10): retract (18,1), emit (18,101).
        Tick((18, 100, 1));
        Assert.Equal(new Z64(-1), spine.Output.Current.WeightOf((18L, 1L)));
        Assert.Equal(Z64.One, spine.Output.Current.WeightOf((18L, 101L)));

        for (long t = 21; t <= 40; t++)
        {
            Tick((t, 1, 1));
        }

        Assert.True(spine.Op.RetainedGroupCount <= (int)Lateness + 1);
        Assert.Equal(flat.Op.RetainedGroupCount, spine.Op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_WithoutFrontierAdvance_RetainsEverything()
    {
        var frontier = new MutableFrontier();
        var s = BuildSpine(frontier);

        for (long t = 0; t < 50; t++)
        {
            s.Input.Push(Delta((t, 1, 1)));
            s.Circuit.Step();
        }

        Assert.Equal(50, s.Op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_RunsOnEmptyDeltaTick_AfterFrontierAdvance()
    {
        // Regression — see the matching flat LatenessGcTests entry.
        var frontier = new MutableFrontier();
        var s = BuildSpine(frontier);

        for (long t = 0; t < 20; t++)
        {
            s.Input.Push(Delta((t, 1, 1)));
            s.Circuit.Step();
        }

        Assert.Equal(20, s.Op.RetainedGroupCount);

        frontier.AdvanceTo(15);
        s.Input.Push(ZSet<Event, Z64>.Empty);
        s.Circuit.Step();

        Assert.Equal(5, s.Op.RetainedGroupCount);
    }

    // ----------------------------------------------------------------
    // Per-batch GC tests for the compaction-folded DropKeysBelow path.
    //
    // The old impl was a full O(retained) rebuild — every call collapsed
    // every level into a single batch at level 0. The new impl dispatches
    // per batch via the precomputed MinMonotoneKey / MaxMonotoneKey:
    // whole-batch drop, keep-as-is, or mixed mask-filter. These tests
    // verify the new shape — batch structure and level layout survive
    // when only some batches are sub-frontier.
    // ----------------------------------------------------------------

    [Fact]
    public void SpineGc_PerBatchDispatch_DropsOldestPreservesYoungerInPlace()
    {
        // BatchesPerLevel=5 means level 0 holds up to 4 batches before
        // compaction fires — enough headroom to integrate 3 ticks at the
        // same level for the test.
        var strategy = new TieredCompactionStrategy(5);
        var frontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                compactionStrategy: strategy,
                frontier: frontier, monotoneKey: k => k);
            b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();

        // Three disjoint time ranges, each its own batch at level 0.
        ih!.Push(Delta((0, 1, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1)));
        circuit.Step();
        ih.Push(Delta((10, 1, 1), (11, 1, 1), (12, 1, 1), (13, 1, 1), (14, 1, 1)));
        circuit.Step();
        ih.Push(Delta((20, 1, 1), (21, 1, 1), (22, 1, 1), (23, 1, 1), (24, 1, 1)));
        circuit.Step();

        Assert.Equal(3, op.Trace.BatchCount);
        Assert.Equal(1, op.Trace.LevelCount);
        Assert.Equal(15, op.RetainedGroupCount);

        // Advance frontier past the oldest batch's max (4) but below the
        // next batch's min (10). The OLDEST batch drops wholesale;
        // batches [10..14] and [20..24] survive untouched at their
        // original level.
        frontier.AdvanceTo(10);
        ih.Push(ZSet<Event, Z64>.Empty);
        circuit.Step();

        Assert.Equal(2, op.Trace.BatchCount);
        Assert.Equal(1, op.Trace.LevelCount);
        Assert.Equal(10, op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_MixedBatch_DropsSubFrontierGroupsOnly()
    {
        // BatchesPerLevel=5: one batch with a mix of below- and above-
        // frontier keys exercises the mixed-batch mask-filter path.
        var strategy = new TieredCompactionStrategy(5);
        var frontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                compactionStrategy: strategy,
                frontier: frontier, monotoneKey: k => k);
            b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();

        // One batch spanning keys [0..9] — entirely mixed once frontier=5.
        ih!.Push(Delta(
            (0, 1, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1),
            (5, 1, 1), (6, 1, 1), (7, 1, 1), (8, 1, 1), (9, 1, 1)));
        circuit.Step();

        Assert.Equal(1, op.Trace.BatchCount);
        Assert.Equal(10, op.RetainedGroupCount);

        frontier.AdvanceTo(5);
        ih.Push(ZSet<Event, Z64>.Empty);
        circuit.Step();

        // Mixed-batch filter retains the batch at its original level
        // with only above-frontier groups (5..9 = 5 keys).
        Assert.Equal(1, op.Trace.BatchCount);
        Assert.Equal(1, op.Trace.LevelCount);
        Assert.Equal(5, op.RetainedGroupCount);
    }

    [Fact]
    public async Task SpineGc_WholeBatchDrop_DeletesSpilledFileButPreservesSurvivors()
    {
        var fs = new InMemoryTableFileSystem();
        var codec = new JsonIndexedLongCodec();
        // MinSpillLevel=1: anything beyond L0 spills to disk.
        var spill = new SpineIndexedSpillConfig<long, long, Z64>
        {
            FileSystem = fs,
            Prefix = "spill",
            Codec = codec,
            MinSpillLevel = 1,
        };
        // BatchesPerLevel=4 — every 4 L0 batches collapse to a single L1
        // batch (spilled). Three rounds of 4 ticks give 3 spilled batches
        // at L1 with disjoint monotone-key ranges; L1 stays below the
        // tier threshold so they don't cascade further.
        var strategy = new TieredCompactionStrategy(4);
        var frontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? ih = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                compactionStrategy: strategy,
                spillConfig: spill,
                frontier: frontier, monotoneKey: k => k);
            b.Output(aggregated);
        });
        var op = circuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();

        // 12 ticks of 4 keys each: keys[step*10 .. step*10+3] per tick.
        // After every 4 ticks, L0 is promoted to one L1 batch (spilled).
        // After 12 ticks: 3 spilled batches at L1 with key ranges
        // [0..33], [40..73], [80..113].
        for (var step = 0; step < 12; step++)
        {
            var baseT = step * 10L;
            ih!.Push(Delta((baseT, 1, 1), (baseT + 1, 1, 1), (baseT + 2, 1, 1), (baseT + 3, 1, 1)));
            circuit.Step();
        }

        // Snapshot the spill file paths BEFORE GC.
        var filesBefore = new List<string>();
        await foreach (var entry in fs.ListAsync("spill/"))
        {
            filesBefore.Add(entry.Path);
        }

        // 3 spilled batches at L1 — confirm setup.
        Assert.Equal(3, filesBefore.Count);

        // Advance the frontier past the OLDEST spilled batch's max (33)
        // but well below the next batch's min (40). This whole-batch-
        // drops L1[0] (file deleted) and leaves L1[1] / L1[2] in place
        // with their spill files.
        frontier.AdvanceTo(40);
        ih!.Push(ZSet<Event, Z64>.Empty);
        circuit.Step();

        var filesAfter = new List<string>();
        await foreach (var entry in fs.ListAsync("spill/"))
        {
            filesAfter.Add(entry.Path);
        }

        // Two spill files survive (L1[1] = keys 40..73, L1[2] = keys 80..113).
        Assert.Equal(2, filesAfter.Count);
        // Surviving groups = keys ≥ 40 from the 12 streamed ticks.
        var expectedSurvivors = Enumerable.Range(0, 12 * 4)
            .Select(i => (i / 4) * 10L + (i % 4))
            .Count(t => t >= 40);
        Assert.Equal(expectedSurvivors, op.RetainedGroupCount);
    }

    [Fact]
    public async Task SpineGc_PostGcSnapshotRoundTripsToIdenticalState()
    {
        // End-to-end witness: integrate → GC → save snapshot → restore
        // into a fresh op → assert the restored trace materialises to
        // the same IndexedZSet, and re-running GC on the restored op is
        // a no-op (every sub-frontier key was already dropped).
        var fs = new InMemoryTableFileSystem();
        var codec = new JsonIndexedLongCodec();

        var producerFrontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? pih = null;
        var producerCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            pih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                snapshotCodec: codec,
                frontier: producerFrontier, monotoneKey: k => k);
            b.Output(aggregated);
        });
        var producerOp = producerCircuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();

        // Stream 30 ticks with frontier following at lag 10; GC fires
        // every tick the frontier advances.
        for (long t = 0; t < 30; t++)
        {
            pih!.Push(Delta((t, 1, 1)));
            producerFrontier.AdvanceTo(t - 10);
            producerCircuit.Step();
        }

        var liveMaterialized = producerOp.Trace.Materialize();
        var liveRetained = producerOp.RetainedGroupCount;

        await SaveOpAsync(producerCircuit, fs);

        var consumerFrontier = new MutableFrontier();
        consumerFrontier.AdvanceTo(producerFrontier.Value);
        InputHandle<ZSet<Event, Z64>>? cih = null;
        var consumerCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            cih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                snapshotCodec: codec,
                frontier: consumerFrontier, monotoneKey: k => k);
            b.Output(aggregated);
        });
        var consumerOp = consumerCircuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();

        await LoadOpAsync(consumerCircuit, fs);

        Assert.Equal(liveRetained, consumerOp.RetainedGroupCount);
        Assert.Equal(liveMaterialized, consumerOp.Trace.Materialize());

        // Re-running GC on the restored op at the same frontier must be
        // a no-op: every sub-frontier key was already dropped before
        // the snapshot was taken.
        var preEmptyTickRetained = consumerOp.RetainedGroupCount;
        cih!.Push(ZSet<Event, Z64>.Empty);
        consumerCircuit.Step();
        Assert.Equal(preEmptyTickRetained, consumerOp.RetainedGroupCount);
    }

    private static async ValueTask SaveOpAsync(RootCircuit circuit, InMemoryTableFileSystem fs)
    {
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is ISnapshotable s)
            {
                await s.SaveAsync(new PrefixedContext(fs, "op-" + i + "/"));
            }
        }
    }

    private static async ValueTask LoadOpAsync(RootCircuit circuit, InMemoryTableFileSystem fs)
    {
        for (var i = 0; i < circuit.Operators.Count; i++)
        {
            if (circuit.Operators[i] is ISnapshotable s)
            {
                await s.LoadAsync(new PrefixedContext(fs, "op-" + i + "/"));
            }
        }
    }

    private sealed class PrefixedContext : ISnapshotWriter, ISnapshotReader
    {
        private readonly ITableFileSystem _fs;
        private readonly string _prefix;

        public PrefixedContext(ITableFileSystem fs, string prefix)
        {
            _fs = fs;
            _prefix = prefix;
        }

        public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.CreateAsync(_prefix + filename, overwrite: true, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.OpenReadAsync(_prefix + filename, cancellationToken);

        public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.ExistsAsync(_prefix + filename, cancellationToken);
    }

    // Minimal indexed codec for long→ZSet<long, Z64>, used by the
    // spill-lifecycle and snapshot tests.
    private sealed class JsonIndexedLongCodec : IIndexedZSetTraceCodec<long, long, Z64>
    {
        public string SchemaFingerprint => "test-long-long-z64";

        public async ValueTask SaveAsync(
            ISnapshotWriter writer, string fileName,
            IndexedZSet<long, long, Z64> trace,
            CancellationToken cancellationToken = default)
        {
            var entries = new List<long[]>();
            foreach (var (k, group) in trace)
            {
                foreach (var (v, w) in group)
                {
                    entries.Add(new[] { k, v, w.Value });
                }
            }

            var json = JsonSerializer.SerializeToUtf8Bytes(entries);
            await using var file = await writer.CreateAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            await stream.WriteAsync(json, cancellationToken);
        }

        public async ValueTask<IndexedZSet<long, long, Z64>> LoadAsync(
            ISnapshotReader reader, string fileName,
            CancellationToken cancellationToken = default)
        {
            if (!await reader.ExistsAsync(fileName, cancellationToken))
            {
                return IndexedZSet<long, long, Z64>.Empty;
            }

            await using var file = await reader.OpenReadAsync(fileName, cancellationToken);
            await using var stream = file.AsStream();
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms, cancellationToken);
            var entries = JsonSerializer.Deserialize<List<long[]>>(ms.ToArray()) ?? new();
            var b = new IndexedZSetBuilder<long, long, Z64>();
            foreach (var e in entries)
            {
                b.Add(e[0], e[1], new Z64(e[2]));
            }

            return b.Build();
        }
    }
}
