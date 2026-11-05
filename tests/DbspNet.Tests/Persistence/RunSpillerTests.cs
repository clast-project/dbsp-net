// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Mid-batch spilling, S1a (docs/design-bounded-memory.md §5.9): once a freeze or merge leaves a run of
// at least the threshold without a file, the run is written to a scratch file and its blocks become
// file blocks the cache can evict. Required, on a join with a giant group (spanning blocks in memory):
//   1. every tick's output equals a twin that never spills, with nearly every body evicted;
//   2. spilled runs are file-backed; a checkpoint still writes its own copy and restores exactly;
//   3. a file is deleted once its run is gone, and nothing is left once the program is;
//   4. runs below the threshold are not spilled;
//   5. (§5.20) a file is deleted the moment its run is merged away, with no collection: on disk are
//      exactly the files the circuit's traces list; a lazy reader captured before the merge still
//      reads the merged-away files, and they go once the reader is collected;
//   6. a lease deletes on its last release, waits for live readers, and refuses a hold after delete;
//   7. the startup sweep removes the spill roots of dead processes only.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

[Collection(nameof(BlockedCheckpointTests))] // sets the process-wide cache budget and spiller
public class RunSpillerTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE a (k INT NOT NULL, s VARCHAR, x BIGINT)",
        "CREATE TABLE b (k INT NOT NULL, t VARCHAR)",
        "CREATE VIEW j AS SELECT a.k, a.s, a.x, b.t FROM a JOIN b ON a.k = b.k",
    ];

    private static readonly HashSet<string> Views = new(["j"], StringComparer.Ordinal);

    private static CompiledProgram Compile()
    {
        using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 64);
        return SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);
    }

    // Key 0 is a giant group in the first ticks; later ticks insert and delete around it.
    private static void Tick(CompiledProgram p, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < (tick < 3 ? 400 : 40); i++)
        {
            var giant = tick < 3;
            p.Table("a").Insert(giant ? 0 : rng.Next(1, 12), (giant ? new string('g', 100) : "s") + tick + "-" + i, (long)rng.Next(100));
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 6; i++)
            {
                var giant = tick - 1 < 3;
                p.Table("a").Delete(giant ? 0 : old.Next(1, 12), (giant ? new string('g', 100) : "s") + (tick - 1) + "-" + i, (long)old.Next(100));
            }
        }

        p.Table("b").Insert(rng.Next(0, 12), "t" + rng.Next(3));
        if (tick == 0)
        {
            p.Table("b").Insert(0, "giant");
        }

        p.Step();
    }

    private static string Digest(CompiledProgram p) =>
        string.Join("; ", p.Outputs["j"].CurrentView
            .Select(e => string.Join("|", Enumerable.Range(0, 4).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value)
            .Order(StringComparer.Ordinal));

    private static BlockedIndexedTrace Left(CompiledProgram p) =>
        p.Circuit.Operators.OfType<BlockedJoinOp>().Single().LeftTrace;

    private static int FilesIn(string dir) => Directory.GetFiles(dir, "*.run").Length;

    // Twin digests per tick, computed with nothing installed.
    private static List<string> TwinDigests(int ticks)
    {
        var twin = Compile();
        var digests = new List<string>();
        for (var tick = 0; tick < ticks; tick++)
        {
            Tick(twin, tick);
            digests.Add(Digest(twin));
        }

        return digests;
    }

    private static async Task WithSpillerAsync(long minBytes, Func<RunSpiller, string, Task> body)
    {
        using var layout = StateLayout.Use(true);
        var savedBudget = BlockCache.Budget;
        var savedSpiller = BlockAccess.Spiller;
        var dir = Path.Combine(Path.GetTempPath(), "dbspnet-runspiller-test-" + Guid.NewGuid().ToString("N"));
        var spiller = new RunSpiller(dir, minBytes);
        BlockCache.Budget = 1; // every admitted body is evicted at once
        BlockAccess.Spiller = spiller;
        try
        {
            await body(spiller, dir);
        }
        finally
        {
            BlockAccess.Spiller = savedSpiller;
            BlockCache.Budget = savedBudget;
            BlockCache.Clear();
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task Spilled_runs_are_file_backed_and_change_no_output()
    {
        const int Ticks = 24;
        var expected = TwinDigests(Ticks);
        await WithSpillerAsync(0, async (spiller, dir) =>
        {
            var runs0 = Interlocked.Read(ref RunSpiller.Runs);
            var p = Compile();
            for (var tick = 0; tick < Ticks; tick++)
            {
                Tick(p, tick);
                Assert.Equal(expected[tick], Digest(p));

                // Every run the trace holds has a file, and its blocks are file blocks.
                Assert.All(Left(p).Runs, r =>
                {
                    Assert.NotNull(r.Persisted);
                    Assert.All(r.Blocks, b => Assert.IsType<LazyTraceBlock>(b));
                });
            }

            Assert.True(Interlocked.Read(ref RunSpiller.Runs) > runs0);

            // Files of runs merged away go once their runs are collected; the live ones stay.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            spiller.Sweep();
            Assert.Equal(spiller.Files, FilesIn(dir));
            Assert.True(spiller.Files >= Left(p).Runs.Count);

            // A checkpoint writes its own copy of every run; the restore matches and goes on matching.
            var fs = new InMemoryTableFileSystem();
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: 1);
            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(p), Digest(restored));
            Tick(p, Ticks);
            Tick(restored, Ticks);
            Assert.Equal(Digest(p), Digest(restored));

            // Once the programs are gone, so are their files.
            p = null!;
            restored = null!;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            spiller.Sweep();
            Assert.Equal(0, spiller.Files);
            Assert.Equal(0, FilesIn(dir));
        });
    }

    [Fact]
    public async Task Runs_below_the_threshold_stay_in_memory()
    {
        await WithSpillerAsync(1L << 40, (spiller, dir) =>
        {
            var p = Compile();
            for (var tick = 0; tick < 12; tick++)
            {
                Tick(p, tick);
            }

            Assert.All(Left(p).Runs, r => Assert.Null(r.Persisted));
            Assert.Equal(0, spiller.Files);
            Assert.Equal(0, FilesIn(dir));
            return Task.CompletedTask;
        });
    }

    // Every blocked trace an operator holds (one level of fields, through a Z-set trace's wrapper).
    private static List<BlockedIndexedTrace> Traces(CompiledProgram p)
    {
        var traces = new List<BlockedIndexedTrace>();
        foreach (var op in p.Circuit.Operators)
        {
            for (var t = op.GetType(); t is not null; t = t.BaseType)
            {
                foreach (var f in t.GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.DeclaredOnly))
                {
                    switch (f.GetValue(op))
                    {
                        case BlockedIndexedTrace trace:
                            traces.Add(trace);
                            break;
                        case BlockedZSetTrace z:
                            traces.Add(z.Trace);
                            break;
                    }
                }
            }
        }

        return traces;
    }

    [Fact]
    public async Task A_merged_away_run_file_is_deleted_at_once_without_a_collection()
    {
        const int Ticks = 24;
        var expected = TwinDigests(Ticks);
        await WithSpillerAsync(0, (spiller, dir) =>
        {
            // State traces only: a block-backed delta's files live as long as the delta object (§5.20).
            using var noDeltas = SpillSettings.UseStepSpill(null);
            var released0 = Interlocked.Read(ref RunSpiller.DeletedOnRelease);
            var p = Compile();
            Assert.NotEmpty(Traces(p));
            for (var tick = 0; tick < Ticks; tick++)
            {
                Tick(p, tick);

                // On disk: exactly the files the traces list now — no merged-away file waits for a GC.
                var listed = Traces(p).SelectMany(t => t.Runs).Select(r => r.Lease).OfType<RunFileLease>().Distinct().ToList();
                Assert.All(listed, l => Assert.False(l.Deleted));
                Assert.Equal(listed.Count, FilesIn(dir));
                Assert.Equal(listed.Count, spiller.Files);

                // The census sees them all as state files: no delta, reader or dead bytes.
                var mix = spiller.Census();
                Assert.Equal(listed.Count, mix.Files);
                Assert.Equal(listed.Sum(l => l.Bytes), mix.State);
                Assert.Equal(0, mix.Delta + mix.Reader + mix.Dead);
            }

            Assert.True(Interlocked.Read(ref RunSpiller.DeletedOnRelease) > released0, "no file was deleted on release");
            Assert.Equal(expected[^1], Digest(p));
            return Task.CompletedTask;
        });
    }

    private static readonly Schema RowSchema = new(
    [
        new SchemaColumn("k", new SqlIntegerType(false)),
        new SchemaColumn("s", new SqlVarcharType(null, true)),
    ]);

    private static ZSet<StructuralRow, Z64> Rows(int from, int count)
    {
        var b = new ZSetBuilder<StructuralRow, Z64>();
        for (var i = from; i < from + count; i++)
        {
            b.Add(new StructuralRow(i, Utf8String.Of(new string('r', 40) + i)), Z64.One);
        }

        return b.Build();
    }

    [Fact]
    public async Task A_lazy_reader_keeps_merged_away_files_until_it_is_collected()
    {
        await WithSpillerAsync(0, (spiller, dir) =>
        {
            using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 16, blockEntries: 8);
            var format = TraceBlockFormats.Create(RowSchema, Schema.Empty)!;
            var trace = new BlockedZSetTrace(format);
            for (var d = 0; d < 6; d++)
            {
                trace.Integrate(Rows(d * 20, 20));
            }

            trace.Idle();
            var before = trace.Trace.Runs.Select(r => r.Lease).OfType<RunFileLease>().ToList();
            Assert.NotEmpty(before);

            // A reader captured now, then merges that retire every run it captured; it still reads them.
            var weak = ReadAfterMerges(trace, before);

            // Once the reader is gone, so are its files — and only those.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(weak.TryGetTarget(out _));
            spiller.Sweep();
            Assert.All(before, l => Assert.True(l.Deleted));
            var listed = trace.Trace.Runs.Select(r => r.Lease).OfType<RunFileLease>().Distinct().Count();
            Assert.Equal(listed, FilesIn(dir));
            Assert.Equal(Rows(0, 800).OrderBy(e => (int)e.Key[0]!), trace.EnumerateWhole().OrderBy(e => (int)e.Key[0]!));
            GC.KeepAlive(trace);
            return Task.CompletedTask;
        });
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference<object> ReadAfterMerges(BlockedZSetTrace trace, List<RunFileLease> before)
    {
        var reader = trace.Trace.StreamRows()!;
        for (var d = 6; d < 40; d++)
        {
            trace.Integrate(Rows(d * 20, 20));
        }

        trace.Idle();
        Assert.DoesNotContain(trace.Trace.Runs, r => r.Lease is { } l && before.Contains(l));
        Assert.All(before, l => Assert.False(l.Deleted)); // the reader still holds them

        // Every body was evicted (budget 1): the reader reads the files back, and sees the old state.
        Assert.Equal(Rows(0, 120).OrderBy(e => (int)e.Key[0]!), reader.OrderBy(e => (int)e.Key[0]!));
        return new WeakReference<object>(reader);
    }

    [Fact]
    public async Task An_adopted_deltas_files_outlive_the_delta()
    {
        await WithSpillerAsync(0, (spiller, dir) =>
        {
            using var mode = TraceBlocksMode.Use(join: true, freezeEntries: 1000, blockEntries: 8);
            var format = TraceBlockFormats.Create(RowSchema, Schema.Empty)!;
            var view = new BlockedZSetTrace(format);
            var weak = AdoptSpilledDelta(view, format);

            // The delta is gone and swept; the view's runs read its files, every body evicted.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(weak.TryGetTarget(out _));
            spiller.Sweep();
            Assert.All(view.Trace.Runs, r => Assert.False(r.Lease is { Deleted: true }));
            Assert.Equal(Rows(0, 40).OrderBy(e => (int)e.Key[0]!), view.Trace.StreamRows()!.OrderBy(e => (int)e.Key[0]!));
            for (var k = 0; k < 40; k++)
            {
                Assert.Equal(1L, view.WeightOf(Rows(k, 1).Single().Key).Value);
            }

            GC.KeepAlive(view);
            return Task.CompletedTask;
        });
    }

    // A delta built as one spilled run (adopted as it is, not merged), integrated into the view and
    // dropped. Returns a weak reference to it.
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference<object> AdoptSpilledDelta(BlockedZSetTrace view, ITraceBlockFormat format)
    {
        ZSet<StructuralRow, Z64> delta;
        using (SpillSettings.UseStepSpill(0, 64))
        {
            var b = DeltaSpill.Rows(format, 40);
            foreach (var (row, w) in Rows(0, 40))
            {
                b.Add(row, w);
            }

            delta = b.Build();
        }

        var blocked = Assert.IsType<BlockedRows>(delta.External);
        Assert.Equal(1, blocked.RunCount);
        var adopted0 = Interlocked.Read(ref DeltaSpill.Adopted);
        view.Integrate(delta);
        Assert.Equal(adopted0 + 1, Interlocked.Read(ref DeltaSpill.Adopted));
        Assert.Single(view.Trace.Runs);
        Assert.All(view.Trace.Runs, r => Assert.NotNull(r.Lease));
        return new WeakReference<object>(blocked);
    }

    [Fact]
    public void A_lease_deletes_on_its_last_release_and_waits_for_live_readers()
    {
        var deleted = 0;
        var a = new object();
        var b = new object();
        var lease = new RunFileLease(() => deleted++);
        lease.Hold(a);
        lease.Hold(b);
        lease.Release(a);
        Assert.Equal(0, deleted);
        lease.Release(b);
        Assert.Equal(1, deleted);
        Assert.True(lease.Deleted);
        Assert.Throws<InvalidOperationException>(() => lease.Hold(a));
        Assert.False(lease.TryDelete()); // once only

        // A reader that never releases holds it until collected; a release of a non-holder is a no-op.
        var second = new RunFileLease(() => deleted++);
        HoldByGarbage(second);
        second.Hold(a);
        second.Release(b);
        second.Release(a);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        Assert.True(second.TryDelete());
        Assert.Equal(2, deleted);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void HoldByGarbage(RunFileLease lease) => lease.Hold(new object());

    [Fact]
    public void The_startup_sweep_removes_dead_processes_spill_roots_only()
    {
        var parent = Path.Combine(Path.GetTempPath(), "dbspnet-sweep-test-" + Guid.NewGuid().ToString("N"));
        try
        {
            var dead = Path.Combine(parent, "dbspnet-spill-" + (int.MaxValue - 7));
            var self = Path.Combine(parent, "dbspnet-spill-" + Environment.ProcessId);
            var running = Path.Combine(parent, "dbspnet-spill-1"); // launchd / init
            var foreign = Path.Combine(parent, "dbspnet-spill-abc");
            var other = Path.Combine(parent, "something-" + (int.MaxValue - 7));
            foreach (var d in new[] { dead, self, running, foreign, other })
            {
                Directory.CreateDirectory(Path.Combine(d, "runs"));
                File.WriteAllText(Path.Combine(d, "runs", "x.run"), "x");
            }

            Assert.Equal(1, BlockSpill.SweepStale(parent));
            Assert.False(Directory.Exists(dead));
            Assert.True(Directory.Exists(self));
            Assert.True(Directory.Exists(running));
            Assert.True(Directory.Exists(foreign));
            Assert.True(Directory.Exists(other));
            Assert.Equal(0, BlockSpill.SweepStale(Path.Combine(parent, "missing")));
        }
        finally
        {
            Directory.Delete(parent, recursive: true);
        }
    }
}
