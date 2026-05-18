using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Long-running session test: drives a multi-stateful-op query through
/// many push+step ticks with periodic snapshots and simulated restarts,
/// verifying after every tick that the persistent session emits the
/// same output delta as a parallel reference session run without
/// persistence. Catches drift bugs that only surface across multiple
/// snapshot/restore boundaries.
/// </summary>
public class LifecycleTests : IDisposable
{
    private readonly string _walDir;
    private readonly string _snapshotDir;

    public LifecycleTests()
    {
        var stem = Guid.NewGuid().ToString("N");
        _walDir = Path.Combine(Path.GetTempPath(), "dbspnet-lifecycle-wal-" + stem);
        _snapshotDir = Path.Combine(Path.GetTempPath(), "dbspnet-lifecycle-snap-" + stem);
    }

    public void Dispose()
    {
        if (Directory.Exists(_walDir))
        {
            Directory.Delete(_walDir, recursive: true);
        }

        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static readonly string[] Ddl =
    {
        "CREATE TABLE products (id INT NOT NULL, category VARCHAR(16) NOT NULL)",
        "CREATE TABLE orders (oid INT NOT NULL, pid INT NOT NULL, amt BIGINT NOT NULL)",
    };

    private const string Query =
        "SELECT products.category, SUM(orders.amt) " +
        "FROM orders JOIN products ON orders.pid = products.id " +
        "GROUP BY products.category";

    private static CompiledQuery CompilePersistent()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static CompiledQuery CompileReference()
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    [Fact]
    public void RepeatedSnapshotRestart_MatchesReferenceOutputAtEveryTick()
    {
        // The reference session has no persistence — it just runs every
        // operation linearly and produces the canonical output sequence.
        var reference = CompileReference();

        // The persistent session does the same operations, plus periodic
        // WriteSnapshot calls and simulated restarts. After every tick,
        // its output delta must match the reference's.
        var persistent = CompilePersistent();
        var recorder = new WalRecorder(persistent, _walDir, _snapshotDir);

        try
        {
            // ---- Cycle 1: bootstrap state ----
            InsertBoth(reference, persistent, "products", 1, "electronics");
            InsertBoth(reference, persistent, "products", 2, "books");
            InsertBoth(reference, persistent, "products", 3, "music");
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            InsertBoth(reference, persistent, "orders", 100, 1, 100L);
            InsertBoth(reference, persistent, "orders", 101, 1, 50L);
            InsertBoth(reference, persistent, "orders", 102, 2, 30L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // First snapshot.
            recorder.WriteSnapshot();

            InsertBoth(reference, persistent, "orders", 103, 2, 20L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // ---- Restart 1: dispose recorder, rebuild circuit, reopen ----
            // After restart, persistent.Current is whatever the most
            // recent replayed Step produced (or unset if no replay
            // happened). The robust invariant is "after restart + at
            // least one new Step, outputs match" — that's what proves
            // the restored operator state is correct, since the next
            // delta depends on it.
            recorder.Dispose();
            persistent = CompilePersistent();
            recorder = new WalRecorder(persistent, _walDir, _snapshotDir);

            // ---- Cycle 2: more inserts, retraction, another snapshot ----
            InsertBoth(reference, persistent, "orders", 104, 1, 25L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            DeleteBoth(reference, persistent, "orders", 100, 1, 100L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            recorder.WriteSnapshot();

            InsertBoth(reference, persistent, "orders", 105, 3, 99L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // ---- Restart 2 ----
            recorder.Dispose();
            persistent = CompilePersistent();
            recorder = new WalRecorder(persistent, _walDir, _snapshotDir);

            // ---- Cycle 3: multiple ticks before next snapshot ----
            InsertBoth(reference, persistent, "orders", 106, 2, 15L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            InsertBoth(reference, persistent, "orders", 107, 1, 5L);
            DeleteBoth(reference, persistent, "orders", 102, 2, 30L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // Snapshot mid-cycle (no immediate restart).
            recorder.WriteSnapshot();

            InsertBoth(reference, persistent, "orders", 108, 3, 1L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // Another snapshot back-to-back (no Step between) —
            // exercises the "snapshot after empty post-snapshot WAL"
            // edge case.
            recorder.WriteSnapshot();

            // ---- Restart 3: trivial restart (snapshot at latest tick,
            //                 no post-snapshot ticks to replay).
            //                 Skip the post-restart assertion — output
            //                 stream isn't initialised when no replay
            //                 runs. The next Step recovers the
            //                 reference-equality invariant. ----
            recorder.Dispose();
            persistent = CompilePersistent();
            recorder = new WalRecorder(persistent, _walDir, _snapshotDir);

            // ---- Cycle 4: a fresh product, then more orders ----
            InsertBoth(reference, persistent, "products", 4, "garden");
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            InsertBoth(reference, persistent, "orders", 109, 4, 200L);
            InsertBoth(reference, persistent, "orders", 110, 4, 50L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // After all this churn, the absolute tick counts on both
            // sides must match — the snapshot/restore path preserves
            // tick numbering.
            Assert.Equal(reference.Circuit.TickCount, persistent.Circuit.TickCount);
        }
        finally
        {
            recorder.Dispose();
        }
    }

    [Fact]
    public void RepeatedSnapshots_PrunedFilesDoNotAccumulate()
    {
        // After many WriteSnapshot+Step cycles, the WAL directory should
        // contain only the segments past the latest snapshot, not a
        // growing pile of pruned ones. Verifies the prune-then-rotate
        // flow holds across many iterations.
        var persistent = CompilePersistent();
        using var recorder = new WalRecorder(persistent, _walDir, _snapshotDir);

        persistent.Table("products").Insert(1, "electronics");
        recorder.Step();

        for (var i = 0; i < 10; i++)
        {
            persistent.Table("orders").Insert(i, 1, (long)(i + 1));
            recorder.Step();
            recorder.WriteSnapshot();
        }

        // Each WriteSnapshot rotates to a new segment id; the prior
        // segments get pruned. The current pending segment file is
        // empty (no ticks yet). Only one .arrows file per table should
        // remain on disk.
        var files = Directory.GetFiles(_walDir, "*.arrows");
        Assert.True(files.Length <= 2,
            $"expected at most one segment file per table after prune cascade; got {files.Length}: " +
            string.Join(", ", files.Select(Path.GetFileName)));
    }

    [Fact]
    public void RestartWithoutSnapshot_FullWalReplayMatchesReference()
    {
        // Same scenario but with NO WriteSnapshot calls — pure WAL
        // replay across restarts. Verifies the legacy (A) path still
        // works after the (C) hybrid changes layered onto it.
        var reference = CompileReference();

        var persistent = CompilePersistent();
        var recorder = new WalRecorder(persistent, _walDir);

        try
        {
            InsertBoth(reference, persistent, "products", 1, "electronics");
            InsertBoth(reference, persistent, "products", 2, "books");
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            InsertBoth(reference, persistent, "orders", 100, 1, 100L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);

            // Restart with no snapshot — full WAL replay.
            recorder.Dispose();
            persistent = CompilePersistent();
            recorder = new WalRecorder(persistent, _walDir);

            InsertBoth(reference, persistent, "orders", 101, 2, 30L);
            StepBoth(reference, recorder);
            AssertSameOutput(reference, persistent);
        }
        finally
        {
            recorder.Dispose();
        }
    }

    private static void InsertBoth(CompiledQuery reference, CompiledQuery persistent, string table, params object?[] values)
    {
        reference.Table(table).Insert(values);
        persistent.Table(table).Insert(values);
    }

    private static void DeleteBoth(CompiledQuery reference, CompiledQuery persistent, string table, params object?[] values)
    {
        reference.Table(table).Delete(values);
        persistent.Table(table).Delete(values);
    }

    private static void StepBoth(CompiledQuery reference, WalRecorder recorder)
    {
        reference.Step();
        recorder.Step();
    }

    private static void AssertSameOutput(CompiledQuery reference, CompiledQuery persistent)
    {
        // ZSet has structural equality — same row→weight map.
        Assert.Equal(reference.Current, persistent.Current);
    }
}
