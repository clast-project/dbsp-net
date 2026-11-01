// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Incremental checkpoints (docs/design-durable-state.md §1f, increment 3). With StateLayout.Runs on,
// a trace's first checkpoint writes a base run and each later one only the change since, as one
// more run in a chain the snapshot names by a run list. Required, for serial and parallel
// programs, flat traces with and without LATENESS GC:
//
//   1. every checkpoint restores exactly, and the restored program carries on exactly;
//   2. chains grow by small deltas, and a restored chain is extended rather than rewritten;
//   3. chains are rewritten whole after RunChain.MaxRuns, and the sweep leaves exactly the runs
//      the retained snapshots name;
//   4. a checkpoint that fails part-way loses nothing: the next one still carries every change;
//   5. a crash before any mutation of a delta checkpoint (process crash or power loss) recovers the
//      old or the new checkpoint exactly, and the recovered program checkpoints on from there.
//
// RunCheckpoint.Verify is on for every test here: each chain is read back as it is written and
// compared with the state, so a mutation the change tracking misses fails at that checkpoint.
using System.Text;
using DbspNet.Core.IO;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Persistence.Runs;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class IncrementalCheckpointTests
{
    private static readonly string[] Sql =
    [
        "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)",
        "CREATE TABLE u (a INT NOT NULL, s VARCHAR NOT NULL)",
        "CREATE VIEW agg AS SELECT a, SUM(b) AS s, COUNT(*) AS n FROM t GROUP BY a",
        "CREATE VIEW j AS SELECT t.a, t.b, u.s FROM t JOIN u ON t.a = u.a",
        "CREATE VIEW d AS SELECT DISTINCT b FROM t",
        "CREATE VIEW top AS SELECT a, b FROM (SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) AS rn FROM t) x WHERE rn <= 2",
        "CREATE VIEW lg AS SELECT a, b, LAG(b) OVER (PARTITION BY a ORDER BY b) AS p FROM t",
        "CREATE VIEW rk AS SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) AS r FROM t",
        "CREATE VIEW ws AS SELECT a, b, SUM(b) OVER (PARTITION BY a ORDER BY b RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM t",
    ];

    private static readonly HashSet<string> Views = new(["agg", "j", "d", "top", "lg", "rk", "ws"], StringComparer.Ordinal);

    public IncrementalCheckpointTests() => RunCheckpoint.Verify = true;

    // Inserts and deletes over a small key space, so traces see repeats, cancellations and
    // keys crossing zero.
    private static void Feed(Action<string, object?[], bool> push, int tick)
    {
        var rng = new Random(tick);
        for (var i = 0; i < 20; i++)
        {
            push("t", [rng.Next(0, 8), rng.Next(0, 50)], true);
        }

        if (tick > 0)
        {
            var old = new Random(tick - 1);
            for (var i = 0; i < 5; i++)
            {
                push("t", [old.Next(0, 8), old.Next(0, 50)], false);
            }
        }

        push("u", [rng.Next(0, 8), "s" + rng.Next(0, 5)], true);
    }

    private static void Tick(CompiledProgram p, int tick)
    {
        Feed((table, row, ins) => { if (ins) { p.Table(table).Insert(row); } else { p.Table(table).Delete(row); } }, tick);
        p.Step();
    }

    private static void Tick(ParallelCompiledProgram p, int tick)
    {
        Feed((table, row, ins) => { if (ins) { p.Table(table).Insert(row); } else { p.Table(table).Delete(row); } }, tick);
        p.Step();
    }

    private static string Digest(Func<string, IEnumerable<string>> rows) =>
        string.Join("\n", Views.OrderBy(v => v, StringComparer.Ordinal).Select(v => v + ": " + string.Join("; ", rows(v).OrderBy(x => x, StringComparer.Ordinal))));

    private static string Digest(CompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value));

    private static string Digest(ParallelCompiledProgram p) =>
        Digest(v => p.Outputs[v].CurrentView.Select(e => string.Join("|", Enumerable.Range(0, p.Outputs[v].Schema.Count).Select(i => FormattableString.Invariant($"{e.Key[i]}"))) + "#" + e.Value.Value));

    private static CompiledProgram Compile() => SqlProgram.Compile(Sql, Views, ArrowSqlSnapshotCodecs.Instance);

    // Every run-index line of every directory under a snapshot prefix: (trace file path relative
    // to the snapshot, base entries, delta entries, runs).
    private static async Task<List<(string File, long Base, long Delta, string[] Runs)>> IndexLinesAsync(ITableFileSystem fs, string prefix)
    {
        await CheckpointCleanup.WaitAsync(fs);
        var lines = new List<(string, long, long, string[])>();
        await foreach (var f in fs.ListAsync(prefix))
        {
            if (!f.Path.EndsWith("/" + RunIndex.FileName, StringComparison.Ordinal))
            {
                continue;
            }

            var dir = f.Path[..^RunIndex.FileName.Length];
            foreach (var line in Encoding.UTF8.GetString(await fs.ReadAllBytesAsync(f.Path)).Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = line.Split('\t');
                lines.Add((dir + parts[0], long.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture),
                    long.Parse(parts[2], System.Globalization.CultureInfo.InvariantCulture), parts[5..]));
            }
        }

        return lines;
    }

    private static async Task<string> CurrentAsync(ITableFileSystem fs) =>
        Encoding.UTF8.GetString(await fs.ReadAllBytesAsync("current.txt"));

    // Every chain the current snapshot records, keyed by trace file path within the snapshot.
    private static async Task<Dictionary<string, string[]>> ChainsAsync(ITableFileSystem fs)
    {
        var current = await CurrentAsync(fs);
        return (await IndexLinesAsync(fs, current + "/"))
            .ToDictionary(l => l.File[(current.Length + 1)..], l => l.Runs, StringComparer.Ordinal);
    }

    // The sweep's invariant: runs/ holds exactly the runs the retained snapshots' run indexes name,
    // plus runs pinned by live lazily restored state.
    private static async Task AssertRunsAreExactlyTheReferencedOnesAsync(ITableFileSystem fs)
    {
        var referenced = (await IndexLinesAsync(fs, "snap-")).SelectMany(l => l.Runs).ToHashSet(StringComparer.Ordinal);
        var present = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var f in fs.ListAsync(RunSweep.RunsPrefix))
        {
            present.Add(f.Path);
        }

        // Beyond what the indexes name, only runs a live lazily restored state still reads (RunPins).
        var pinned = RunPins.In(fs);
        var extra = present.Except(referenced).Where(p => !pinned.Contains(p)).ToList();
        var missing = referenced.Except(present).ToList();
        Assert.True(extra.Count == 0 && missing.Count == 0,
            $"runs/ holds {present.Count} runs, the indexes name {referenced.Count}, {pinned.Count} pinned; extra: {string.Join(", ", extra.Take(3))}; missing: {string.Join(", ", missing.Take(3))}");

        // And no trace file is written beside its index entry: one file per directory, not per trace.
        var current = await CurrentAsync(fs);
        foreach (var line in await IndexLinesAsync(fs, current + "/"))
        {
            Assert.False(await fs.ExistsAsync(line.File), $"{line.File} exists as well as in the run index");
        }
    }

    [Fact]
    public async Task ChainsGrowByDeltas_AndEveryCheckpointRestoresExactly()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile();
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(p, tick);
        }

        await Snapshot.WriteAsync(p.Circuit, fs);
        var first = await ChainsAsync(fs);
        Assert.NotEmpty(first);
        Assert.All(first.Values, chain => Assert.Single(chain));

        for (var checkpoint = 1; checkpoint <= 4; checkpoint++)
        {
            Tick(p, 3 + checkpoint);
            await Snapshot.WriteAsync(p.Circuit, fs);
            var chains = await ChainsAsync(fs);

            // Tracked state extends its chain — traces and the partitioned operators alike.
            var extended = chains.Where(c => c.Value.Length == 1 + checkpoint).ToList();
            Assert.True(extended.Count > 0, $"checkpoint {checkpoint}: no chain was extended");
            foreach (var (file, chain) in extended)
            {
                Assert.Equal(first[file][0], chain[0]); // same base run, never rewritten
            }

            await AssertRunsAreExactlyTheReferencedOnesAsync(fs);

            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.Equal(Digest(p), Digest(restored));

            var twin = Compile();
            for (var tick = 0; tick < 4 + checkpoint; tick++)
            {
                Tick(twin, tick);
            }

            for (var tick = 100; tick < 103; tick++)
            {
                Tick(twin, tick);
                Tick(restored, tick);
                Assert.Equal(Digest(twin), Digest(restored));
            }
        }
    }

    [Fact]
    public async Task ARestoredChain_IsExtended_NotRewritten()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var producer = Compile();
        for (var tick = 0; tick < 4; tick++)
        {
            Tick(producer, tick);
        }

        await Snapshot.WriteAsync(producer.Circuit, fs);
        var bases = await ChainsAsync(fs);

        var resumed = Compile();
        await Snapshot.ReadAsync(resumed.Circuit, fs);
        Tick(resumed, 4);
        Tick(producer, 4);
        await Snapshot.WriteAsync(resumed.Circuit, fs);

        var chains = await ChainsAsync(fs);
        var extended = chains.Where(c => c.Value.Length == 2).ToList();
        Assert.True(extended.Count > 0, "the resumed program rewrote every chain");
        foreach (var (file, chain) in extended)
        {
            Assert.Equal(bases[file][0], chain[0]);
        }

        await AssertRunsAreExactlyTheReferencedOnesAsync(fs);
        var again = Compile();
        await Snapshot.ReadAsync(again.Circuit, fs);
        Assert.Equal(Digest(producer), Digest(again));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task LongChains_AreRewritten_AndTheSweepKeepsExactlyTheReferencedRuns(int retain)
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var p = Compile();
        Tick(p, 0);
        for (var checkpoint = 0; checkpoint < RunChain.MaxRuns + 4; checkpoint++)
        {
            Tick(p, 1 + checkpoint);
            await Snapshot.WriteAsync(p.Circuit, fs, retainCount: retain);
            Assert.All((await ChainsAsync(fs)).Values, chain => Assert.InRange(chain.Length, 1, RunChain.MaxRuns));
            await AssertRunsAreExactlyTheReferencedOnesAsync(fs);
        }

        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, fs);
        Assert.Equal(Digest(p), Digest(restored));
    }

    [Fact]
    public async Task AFailedCheckpoint_LosesNoChange()
    {
        // Counts mutations: the cleanup a checkpoint defers must happen inside the count.
        using var inlineCleanup = CheckpointCleanup.Inline();
        // Fail the second checkpoint at each of its mutations in turn (some delta runs written,
        // no commit), carry on in the same process, checkpoint again: the state must restore
        // exactly — and after power loss too, since the failed checkpoint's runs were never synced.
        using var layout = StateLayout.Use(true);
        var mutations = 0;
        for (var failAt = 0; failAt == 0 || failAt < mutations; failAt++)
        {
            var fs = new FailingFileSystem(new InMemoryTableFileSystem());
            var p = Compile();
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(p, tick);
            }

            await Snapshot.WriteAsync(p.Circuit, fs);
            Tick(p, 4);
            var m0 = fs.Mutations;
            if (failAt == 0)
            {
                // Count the second checkpoint's mutations on a copy of the scenario.
                var probe = new FailingFileSystem(new InMemoryTableFileSystem());
                var q = Compile();
                for (var tick = 0; tick < 5; tick++)
                {
                    Tick(q, tick);
                    if (tick == 3)
                    {
                        await Snapshot.WriteAsync(q.Circuit, probe);
                    }
                }

                var before = probe.Mutations;
                await Snapshot.WriteAsync(q.Circuit, probe);
                mutations = probe.Mutations - before;
            }

            fs.FailAfter = m0 + failAt;
            await Assert.ThrowsAsync<IOException>(async () => await Snapshot.WriteAsync(p.Circuit, fs));
            fs.FailAfter = int.MaxValue;

            Tick(p, 5);
            await Snapshot.WriteAsync(p.Circuit, fs);
            await AssertRunsAreExactlyTheReferencedOnesAsync(fs);

            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, fs);
            Assert.True(Digest(p) == Digest(restored), $"fail at {failAt}/{mutations}: restore differs");

            var afterPowerLoss = Compile();
            await Snapshot.ReadAsync(afterPowerLoss.Circuit, await fs.AfterPowerLossAsync());
            Assert.True(Digest(p) == Digest(afterPowerLoss), $"fail at {failAt}/{mutations}: restore after power loss differs");
        }
    }

    // LATENESS is wired on the single-query path (CompiledQuery), not the program path.
    private static CompiledQuery CompileQuery(string[] ddl, string query)
    {
        var catalog = new DbspNet.Sql.Plan.Catalog();
        var resolver = new DbspNet.Sql.Plan.Resolver(catalog);
        foreach (var statement in ddl)
        {
            resolver.Resolve(DbspNet.Sql.Parser.Parser.ParseStatement(statement));
        }

        var plan = ((DbspNet.Sql.Plan.SelectPlan)resolver.Resolve(DbspNet.Sql.Parser.Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    [Theory]
    [InlineData("SELECT a.x, b.x AS y FROM a JOIN b ON a.ts = b.ts")]
    [InlineData("SELECT ts FROM a UNION SELECT ts FROM b")]
    [InlineData("SELECT ts, x, SUM(x) OVER (PARTITION BY x ORDER BY ts RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) AS s FROM a")]
    public async Task LatenessGc_IsCarriedInTheDeltas(string query)
    {
        // State behind a LATENESS frontier is dropped by DropKeysBelow (join: indexed traces;
        // UNION: DISTINCT's Z-set trace) or by the window aggregate's own GC (state outside a trace). The drop must reach the chain like any other change —
        // Verify compares chain and state at every checkpoint — and a delta must hold the drops.
        using var layout = StateLayout.Use(true);
        string[] ddl =
        [
            "CREATE TABLE a (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
            "CREATE TABLE b (ts BIGINT NOT NULL LATENESS 10, x INT NOT NULL)",
        ];
        static void Step(CompiledQuery q, long t)
        {
            q.Table("a").Insert(t, (int)t);
            if (q.Inputs.ContainsKey("b"))
            {
                q.Table("b").Insert(t, (int)(t * 2));
            }

            q.Step();
        }

        // The output delta of the last step (this query path emits deltas, not a stored view).
        static string Delta(CompiledQuery q) =>
            string.Join(";", q.Current.Select(e => e.Key.ToString() + "#" + e.Value.Value).Order(StringComparer.Ordinal));

        var fs = new InMemoryTableFileSystem();
        var p = CompileQuery(ddl, query);
        long time = 0;
        var sawDrops = false;
        for (var checkpoint = 0; checkpoint < 6; checkpoint++)
        {
            for (var i = 0; i < 8; i++, time++)
            {
                Step(p, time);
            }

            await Snapshot.WriteAsync(p.Circuit, fs);

            // After the first checkpoint the frontier passes keys already written, so a delta
            // holds both the 8 new keys and the dropped ones: more entries than it inserted.
            sawDrops |= (await DeltaEntriesAsync(fs)).Any(c => c.Delta > 8L * (c.Runs - 1));

            var restored = CompileQuery(ddl, query);
            await Snapshot.ReadAsync(restored.Circuit, fs);
            var reference = CompileQuery(ddl, query);
            for (var t = 0L; t < time; t++)
            {
                Step(reference, t);
            }

            // Late rows (below the frontier) and new ones: every step's output must match, which
            // needs both the retained state and the GC frontier restored exactly.
            for (var t = time - 12; t < time + 15; t++)
            {
                Step(restored, t);
                Step(reference, t);
                Assert.Equal(Delta(reference), Delta(restored));
            }
        }

        Assert.True(sawDrops, "no delta carried a GC drop");
    }

    // Per chain in the current snapshot: its run count and the entries across its deltas.
    private static async Task<List<(int Runs, long Delta)>> DeltaEntriesAsync(InMemoryTableFileSystem fs) =>
        (await IndexLinesAsync(fs, await CurrentAsync(fs) + "/")).Select(l => (l.Runs.Length, l.Delta)).ToList();

    [Fact]
    public async Task ParallelChains_RestoreAndReshard()
    {
        using var layout = StateLayout.Use(true);
        var fs = new InMemoryTableFileSystem();
        var reference = Compile();
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 3, out var producer, ArrowSqlSnapshotCodecs.Instance));
        using (producer)
        {
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(reference, tick);
                Tick(producer!, tick);
            }

            await ParallelSnapshot.WriteAsync(producer!.Circuit, fs);
            for (var tick = 4; tick < 7; tick++)
            {
                Tick(reference, tick);
                Tick(producer!, tick);
                await ParallelSnapshot.WriteAsync(producer!.Circuit, fs);
                Assert.Contains((await ChainsAsync(fs)).Values, chain => chain.Length > 1);
                await AssertRunsAreExactlyTheReferencedOnesAsync(fs);
            }
        }

        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 3, out var same, ArrowSqlSnapshotCodecs.Instance));
        using (same)
        {
            await ParallelSnapshot.ReadAsync(same!.Circuit, fs);
            Assert.Equal(Digest(reference), Digest(same));
        }

        // Re-shard 3 -> 2 reads the chains and writes self-contained runs; then checkpoint on.
        Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 2, out var restored, ArrowSqlSnapshotCodecs.Instance));
        using (restored)
        {
            Assert.Equal(3, await ParallelReshard.RewriteAsync(fs, restored!));

            // Increment 7: the re-shard writes chains (base runs + run indexes), so the restore
            // after it is lazy and the checkpoints after it extend them rather than rewrite.
            var reshardChains = await ChainsAsync(fs);
            Assert.NotEmpty(reshardChains);
            Assert.All(reshardChains.Values, chain => Assert.Single(chain));

            await ParallelSnapshot.ReadAsync(restored!.Circuit, fs);
            Assert.Equal(Digest(reference), Digest(restored));
            await AssertRunsAreExactlyTheReferencedOnesAsync(fs);

            for (var tick = 7; tick < 10; tick++)
            {
                Tick(reference, tick);
                Tick(restored, tick);
                await ParallelSnapshot.WriteAsync(restored.Circuit, fs);
                await AssertRunsAreExactlyTheReferencedOnesAsync(fs);
            }

            var extended = (await ChainsAsync(fs)).Where(c => reshardChains.TryGetValue(c.Key, out var b) && c.Value.Length > 1 && c.Value[0] == b[0]).ToList();
            Assert.True(extended.Count > 0, "no chain the re-shard wrote was extended by a later checkpoint");

            Assert.True(SqlProgram.TryCompileParallel(Sql, Views, 2, out var last, ArrowSqlSnapshotCodecs.Instance));
            using (last)
            {
                await ParallelSnapshot.ReadAsync(last!.Circuit, fs);
                Assert.Equal(Digest(reference), Digest(last));
            }
        }
    }

    [Fact]
    public async Task CrashAtEveryMutationOfADeltaCheckpoint_RecoversTheOldOrTheNew()
    {
        // Counts mutations: the cleanup a checkpoint defers must happen inside the count.
        using var inlineCleanup = CheckpointCleanup.Inline();
        using var layout = StateLayout.Use(true);
        string Oracle(int ticks)
        {
            var q = Compile();
            for (var tick = 0; tick < ticks; tick++)
            {
                Tick(q, tick);
            }

            return Digest(q);
        }

        var before = Oracle(4);
        var after = Oracle(6);

        long mutations;
        {
            var fs = new CrashSimulatingFileSystem();
            var p = Compile();
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(p, tick);
            }

            await Snapshot.WriteAsync(p.Circuit, fs);
            Tick(p, 4);
            Tick(p, 5);
            var m0 = fs.Mutations;
            await Snapshot.WriteAsync(p.Circuit, fs);
            mutations = fs.Mutations - m0;
        }

        for (var k = 0L; k <= mutations; k++)
        {
            var fs = new CrashSimulatingFileSystem();
            var producer = Compile();
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(producer, tick);
            }

            await Snapshot.WriteAsync(producer.Circuit, fs);
            Tick(producer, 4);
            Tick(producer, 5);
            fs.CrashAt = fs.Mutations + k;
            var completed = true;
            try
            {
                await Snapshot.WriteAsync(producer.Circuit, fs);
            }
            catch (SimulatedCrashException)
            {
                completed = false;
            }

            foreach (var powerLoss in new[] { false, true })
            {
                var restart = powerLoss ? await fs.AfterPowerLossAsync() : await fs.AfterProcessCrashAsync();
                var where = $"crash before mutation {k}/{mutations} " + (powerLoss ? "(power loss)" : "(process crash)");
                var recovered = Compile();
                await Snapshot.ReadAsync(recovered.Circuit, restart);
                var tick = recovered.Circuit.TickCount;
                var digest = Digest(recovered);
                Assert.True(digest == before || digest == after, $"{where}: restored state is neither checkpoint (tick {tick})");
                if (completed && k == mutations)
                {
                    Assert.True(digest == after, $"{where}: the completed checkpoint did not survive");
                }

                // The recovered program checkpoints on — extending the restored chains — and that
                // checkpoint restores exactly too.
                var resumeAt = digest == after ? 6 : 4;
                Tick(recovered, resumeAt);
                await Snapshot.WriteAsync(recovered.Circuit, restart);
                await AssertRunsAreExactlyTheReferencedOnesAsync(restart);
                var again = Compile();
                await Snapshot.ReadAsync(again.Circuit, restart);
                Assert.True(Digest(recovered) == Digest(again), $"{where}: checkpoint after recovery restores differently");
            }
        }
    }

    [Fact]
    public async Task ACrashDuringDeferredCleanup_StillRecoversTheNewCheckpoint()
    {
        // Cleanup (prune + sweep) runs after the commit, in the background. A crash in it — at any of
        // its first mutations — must leave the just-committed checkpoint restorable, after a
        // process crash or power loss, and the next checkpoint must finish the cleanup.
        using var layout = StateLayout.Use(true);
        string after;
        {
            var q = Compile();
            for (var tick = 0; tick < 6; tick++)
            {
                Tick(q, tick);
            }

            after = Digest(q);
        }

        for (var extra = 0; extra < 6; extra++)
        {
            var fs = new CrashSimulatingFileSystem();
            var p = Compile();
            for (var tick = 0; tick < 4; tick++)
            {
                Tick(p, tick);
            }

            await Snapshot.WriteAsync(p.Circuit, fs);
            await CheckpointCleanup.WaitAsync(fs);
            Tick(p, 4);
            Tick(p, 5);

            // How many mutations the checkpoint makes before returning (its cleanup deferred):
            // crash at that one plus `extra`, i.e. inside the cleanup.
            var probe = new CrashSimulatingFileSystem();
            var twin = Compile();
            for (var tick = 0; tick < 6; tick++)
            {
                Tick(twin, tick);
                if (tick == 3)
                {
                    await Snapshot.WriteAsync(twin.Circuit, probe);
                    await CheckpointCleanup.WaitAsync(probe);
                }
            }

            var before = probe.Mutations;
            await Snapshot.WriteAsync(twin.Circuit, probe);
            var returned = probe.Mutations - before;
            await CheckpointCleanup.WaitAsync(probe);

            fs.CrashAt = fs.Mutations + returned + extra;
            await Snapshot.WriteAsync(p.Circuit, fs); // returns: the crash is in the cleanup
            await CheckpointCleanup.WaitAsync(fs);

            foreach (var powerLoss in new[] { false, true })
            {
                var restart = powerLoss ? await fs.AfterPowerLossAsync() : await fs.AfterProcessCrashAsync();
                var recovered = Compile();
                await Snapshot.ReadAsync(recovered.Circuit, restart);
                Assert.True(after == Digest(recovered), $"crash at cleanup mutation {extra} ({(powerLoss ? "power loss" : "process crash")}): not the new checkpoint");

                Tick(recovered, 6);
                await Snapshot.WriteAsync(recovered.Circuit, restart);
                await AssertRunsAreExactlyTheReferencedOnesAsync(restart);
                var again = Compile();
                await Snapshot.ReadAsync(again.Circuit, restart);
                Assert.Equal(Digest(recovered), Digest(again));
            }
        }
    }

    /// <summary>An in-memory store whose mutations start failing (IOException) after a count, and
    /// which keeps a durable image (only what was synced) for a power-loss restart.</summary>
    private sealed class FailingFileSystem(InMemoryTableFileSystem inner) : IDurableTableFileSystem
    {
        private readonly Dictionary<string, byte[]> _durable = new(StringComparer.Ordinal);
        private int _mutations;

        public int FailAfter { get; set; } = int.MaxValue;

        public int Mutations => Volatile.Read(ref _mutations);

        public async Task<InMemoryTableFileSystem> AfterPowerLossAsync()
        {
            var fs = new InMemoryTableFileSystem();
            foreach (var (path, bytes) in _durable)
            {
                await fs.WriteAllBytesAsync(path, bytes);
            }

            return fs;
        }

        private void Mutate()
        {
            if (Interlocked.Increment(ref _mutations) > FailAfter)
            {
                throw new IOException("injected failure");
            }
        }

        public async ValueTask SyncAsync(IReadOnlyCollection<string> paths, CancellationToken cancellationToken = default)
        {
            foreach (var path in paths)
            {
                if (await inner.ExistsAsync(path, cancellationToken))
                {
                    _durable[path] = await inner.ReadAllBytesAsync(path, cancellationToken);
                }
            }
        }

        public async ValueTask WriteAllBytesDurableAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Mutate();
            await inner.WriteAllBytesAsync(path, data, cancellationToken);
            _durable[path] = data.ToArray();
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
            inner.OpenReadAsync(path, cancellationToken);

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            Mutate();
            return inner.CreateAsync(path, overwrite, cancellationToken);
        }

        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
        {
            Mutate();
            return inner.RenameAsync(sourcePath, targetPath, cancellationToken);
        }

        public async ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
        {
            await inner.DeleteAsync(path, cancellationToken);
            _durable.Remove(path);
        }

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ReadAllBytesAsync(path, cancellationToken);

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Mutate();
            return inner.WriteAllBytesAsync(path, data, cancellationToken);
        }
    }
}
