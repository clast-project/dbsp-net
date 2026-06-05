// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Phase 5 — per-worker snapshots. A <see cref="ParallelTypedCompiledQuery"/>'s W
/// replicas each snapshot their stateful operators into a <c>worker-{w}/</c>
/// subtree; recovery requires the same W (the stable hash partition is part of
/// the persisted state). Snapshot a parallel group-by, rebuild a fresh parallel
/// query, restore, and verify subsequent ticks match a reference that ran
/// uninterrupted.
/// </summary>
public class ParallelSnapshotTests
{
    private static LogicalPlan CompilePlan(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        return ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
    }

    private static ParallelTypedCompiledQuery CompileParallel(LogicalPlan plan, int workers)
    {
        Assert.True(
            TypedPlanCompiler.TryCompileParallel(plan, workers, out var q, ArrowSqlSnapshotCodecs.Instance),
            "parallel compile failed");
        return q!;
    }

    private static Dictionary<string, long> Materialize(IEnumerable<(object?[] Values, long Weight)> current)
    {
        var map = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var (values, weight) in current)
        {
            var key = string.Join("|", values.Select(v => v?.ToString() ?? "<null>"));
            map[key] = map.GetValueOrDefault(key) + weight;
            if (map[key] == 0)
            {
                map.Remove(key);
            }
        }

        return map;
    }

    private static readonly string[] GroupByDdl = ["CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)"];
    private const string GroupByQuery = "SELECT k, COUNT(*), SUM(v) FROM t GROUP BY k";

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public async Task SnapshotAndRestore_GroupBy_ContinuesAsIfUninterrupted(int workers)
    {
        var plan = CompilePlan(GroupByDdl, GroupByQuery);
        var fs = new InMemoryTableFileSystem();

        // Reference runs uninterrupted across all three ticks; the subject is
        // snapshotted after tick 2, rebuilt, and restored before tick 3.
        using var reference = CompileParallel(plan, workers);
        using var producer = CompileParallel(plan, workers);

        void Tick1(ParallelTypedCompiledQuery q)
        {
            q.Table("t").Insert(1, 10L);
            q.Table("t").Insert(1, 20L);
            q.Table("t").Insert(2, 5L);
            q.Table("t").Insert(3, 7L);
        }

        void Tick2(ParallelTypedCompiledQuery q)
        {
            q.Table("t").Insert(2, 5L);
            q.Table("t").Delete(1, 10L);
            q.Table("t").Insert(40, 99L);
        }

        Tick1(reference); reference.Step();
        Tick1(producer); producer.Step();
        Tick2(reference); reference.Step();
        Tick2(producer); producer.Step();

        Assert.Equal(Materialize(reference.Current), Materialize(producer.Current));

        // Snapshot the producer, then drop it and rebuild from the same plan.
        var saved = await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        Assert.True(saved > 0, "expected at least one operator snapshotted across workers");
        Assert.True(await ParallelSnapshot.ExistsAsync(fs));
        producer.Dispose();

        using var consumer = CompileParallel(plan, workers);
        var restored = await ParallelSnapshot.ReadAsync(consumer.Circuit, fs);
        Assert.Equal(saved, restored);
        Assert.Equal(reference.Circuit.TickCount, consumer.Circuit.TickCount);

        // Tick 3 on both: the restored consumer must produce the same delta as the
        // reference that never stopped.
        void Tick3(ParallelTypedCompiledQuery q)
        {
            q.Table("t").Insert(4, 100L);
            q.Table("t").Delete(3, 7L);
            q.Table("t").Insert(2, 1L);
        }

        Tick3(reference); reference.Step();
        Tick3(consumer); consumer.Step();

        Assert.Equal(Materialize(reference.Current), Materialize(consumer.Current));
    }

    [Fact]
    public async Task Restore_RejectsDifferentWorkerCount()
    {
        var plan = CompilePlan(GroupByDdl, GroupByQuery);
        var fs = new InMemoryTableFileSystem();

        using (var producer = CompileParallel(plan, 4))
        {
            producer.Table("t").Insert(1, 10L);
            producer.Step();
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        using var consumer = CompileParallel(plan, 2);
        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            async () => await ParallelSnapshot.ReadAsync(consumer.Circuit, fs));
        Assert.Contains("4 workers", ex.Message);
        Assert.Contains("has 2", ex.Message);
    }

    [Fact]
    public async Task Restore_WithoutSnapshot_Throws()
    {
        var plan = CompilePlan(GroupByDdl, GroupByQuery);
        var fs = new InMemoryTableFileSystem();
        using var consumer = CompileParallel(plan, 4);

        Assert.False(await ParallelSnapshot.ExistsAsync(fs));
        await Assert.ThrowsAsync<FileNotFoundException>(
            async () => await ParallelSnapshot.ReadAsync(consumer.Circuit, fs));
    }

    [Fact]
    public async Task Snapshot_LaysOutPerWorkerSubtrees_AndMarkerLast()
    {
        const int workers = 4;
        var plan = CompilePlan(GroupByDdl, GroupByQuery);
        var fs = new InMemoryTableFileSystem();

        using var producer = CompileParallel(plan, workers);
        producer.Table("t").Insert(7, 1L);
        producer.Step();
        await ParallelSnapshot.WriteAsync(producer.Circuit, fs);

        // One marker plus a self-contained single-circuit snapshot per worker.
        Assert.True(await fs.ExistsAsync("parallel.json"));
        for (var w = 0; w < workers; w++)
        {
            Assert.True(await fs.ExistsAsync($"worker-{w}/current.txt"),
                $"worker {w} should have its own snapshot pointer");
        }

        var manifest = await ParallelSnapshotManifest.ReadAsync(fs, "parallel.json");
        Assert.Equal(workers, manifest.Workers);
        Assert.Equal(producer.Circuit.TickCount, manifest.Tick);
    }
}
