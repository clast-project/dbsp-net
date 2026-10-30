// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// The stored / integrated output (<c>IntegrateOp</c>) persists the full
/// materialized view as part of the circuit snapshot — the analogue of Feldera's
/// <c>+stored</c> materialized view being retained across a restart. A restored
/// circuit continues from the saved view (a retraction after restore touches rows
/// the consumer never inserted directly).
/// </summary>
public class IntegrateSnapshotTests : IDisposable
{
    private const string Ddl = "CREATE TABLE t (a INT NOT NULL, b INT NOT NULL)";
    private const string Query = "SELECT a, b FROM t WHERE b > 0";

    private static readonly CompileOptions Stored = new() { StoredOutput = true };

    private readonly string _snapshotDir;

    public IntegrateSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-integrate-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(ISqlSnapshotCodecs? codecs)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan, codecs, Stored);
    }

    [Fact]
    public async Task RestoredView_ContinuesFromPersistedState()
    {
        // Producer builds a view of three rows, then snapshots.
        var producer = Compile(ArrowSqlSnapshotCodecs.Instance);
        producer.Table("t").Insert(1, 10);
        producer.Table("t").Insert(2, 20);
        producer.Table("t").Insert(3, 30);
        producer.Step();
        Assert.Equal(3, producer.CurrentView.Count);

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(ArrowSqlSnapshotCodecs.Instance);
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);

        // Delete a row the consumer never inserted, and add a new one. Proof the
        // full view round-tripped: the delete lands, leaving 2 rows + 1 new = 3.
        consumer.Table("t").Delete(2, 20);
        consumer.Table("t").Insert(4, 40);
        consumer.Step();

        Assert.Equal(3, consumer.CurrentView.Count);
        Assert.Equal(1, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 1, 10 })).Value);
        Assert.Equal(0, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 2, 20 })).Value);
        Assert.Equal(1, consumer.CurrentView.WeightOf(new StructuralRow(new object?[] { 4, 40 })).Value);

        // Cross-check against an uninterrupted run of the same total sequence.
        var control = Compile(ArrowSqlSnapshotCodecs.Instance);
        control.Table("t").Insert(1, 10);
        control.Table("t").Insert(2, 20);
        control.Table("t").Insert(3, 30);
        control.Step();
        control.Table("t").Delete(2, 20);
        control.Table("t").Insert(4, 40);
        control.Step();
        Assert.True(consumer.CurrentView.Equals(control.CurrentView));
    }

    [Fact]
    public async Task NoCodec_ThrowsOnSnapshot()
    {
        var query = Compile(codecs: null); // StoredOutput but no snapshot codec
        query.Table("t").Insert(1, 10);
        query.Step();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await Snapshot.WriteAsync(query.Circuit, _snapshotDir));
    }

    [Fact]
    public async Task PagedViewStore_WritesTheSameSnapshotBytes_AndRestoresEitherWay()
    {
        // docs/design-paged-trace-state.md §7: the paged store saves through the same codec in
        // the same row order, so a snapshot from either representation restores into either.
        async Task<(string Dir, CompiledQuery Query)> Run(bool paged, string name)
        {
            var dir = Path.Combine(_snapshotDir, name);
            var previous = TraceStoreMode.Paged;
            TraceStoreMode.Paged = paged;
            try
            {
                var q = Compile(ArrowSqlSnapshotCodecs.Instance);
                for (var i = 0; i < 50; i++)
                {
                    q.Table("t").Insert(i, i + 1);
                }

                q.Step();
                for (var i = 0; i < 50; i += 3)
                {
                    q.Table("t").Delete(i, i + 1);
                }

                q.Table("t").Insert(100, 7);
                q.Step();
                await Snapshot.WriteAsync(q.Circuit, dir);
                return (dir, q);
            }
            finally
            {
                TraceStoreMode.Paged = previous;
            }
        }

        var (boxedDir, boxed) = await Run(paged: false, "boxed");
        var (pagedDir, paged) = await Run(paged: true, "paged");
        Assert.True(boxed.CurrentView.Equals(paged.CurrentView));

        var boxedFiles = Directory.GetFiles(boxedDir, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(boxedDir, f)).Order(StringComparer.Ordinal).ToList();
        var pagedFiles = Directory.GetFiles(pagedDir, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(pagedDir, f)).Order(StringComparer.Ordinal).ToList();
        Assert.Equal(boxedFiles, pagedFiles);
        Assert.Contains(boxedFiles, f => f.EndsWith("view.arrows", StringComparison.Ordinal));
        foreach (var f in boxedFiles.Where(f => f.EndsWith("view.arrows", StringComparison.Ordinal)))
        {
            Assert.Equal(File.ReadAllBytes(Path.Combine(boxedDir, f)), File.ReadAllBytes(Path.Combine(pagedDir, f)));
        }

        foreach (var (dir, intoPaged) in new[] { (boxedDir, true), (pagedDir, false) })
        {
            var previous = TraceStoreMode.Paged;
            TraceStoreMode.Paged = intoPaged;
            try
            {
                var restored = Compile(ArrowSqlSnapshotCodecs.Instance);
                await Snapshot.ReadAsync(restored.Circuit, dir);
                restored.Table("t").Delete(1, 2);
                restored.Step();
                boxed.Table("t").Delete(1, 2);
                boxed.Step();
                Assert.True(restored.CurrentView.Equals(boxed.CurrentView));
                boxed.Table("t").Insert(1, 2);
                boxed.Step();
            }
            finally
            {
                TraceStoreMode.Paged = previous;
            }
        }
    }

    [Fact]
    public async Task PagedJoinStore_WritesTheSameSnapshotBytes()
    {
        // Increment 3: an inner join's two traces in the paged store save through the same codecs
        // in the same group and value order, so every file matches byte for byte.
        async Task<string> Run(bool paged, string name)
        {
            var dir = Path.Combine(_snapshotDir, name);
            var previous = TraceStoreMode.Paged;
            TraceStoreMode.Paged = paged;
            try
            {
                var catalog = new Catalog();
                var resolver = new Resolver(catalog);
                resolver.Resolve(Parser.ParseStatement("CREATE TABLE l (k INT NOT NULL, a VARCHAR, x DOUBLE)"));
                resolver.Resolve(Parser.ParseStatement("CREATE TABLE r (k INT NOT NULL, b VARCHAR)"));
                var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(
                    "SELECT l.k, a, x, b FROM l JOIN r ON l.k = r.k"))).Query;
                // ShareArrangements skips the typed fast path (the store is structural-only); with
                // snapshot codecs present no arrangement is actually shared.
                var q = PlanToCircuit.Compile(
                    plan, ArrowSqlSnapshotCodecs.Instance, new CompileOptions { StoredOutput = true, ShareArrangements = true });
                for (var i = 0; i < 60; i++)
                {
                    q.Table("l").Insert(i % 7, "a" + i, i * 0.5);
                    q.Table("r").Insert(i % 5, "b" + (i % 3));
                }

                q.Step();
                for (var i = 0; i < 60; i += 4)
                {
                    q.Table("l").Delete(i % 7, "a" + i, i * 0.5);
                }

                q.Table("r").Delete(1, "b1");
                q.Table("l").Insert(3, null, -0.0);
                q.Step();
                await Snapshot.WriteAsync(q.Circuit, dir);
                return dir;
            }
            finally
            {
                TraceStoreMode.Paged = previous;
            }
        }

        var boxedDir = await Run(paged: false, "join-boxed");
        var pagedDir = await Run(paged: true, "join-paged");
        var files = Directory.GetFiles(boxedDir, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(boxedDir, f)).Order(StringComparer.Ordinal).ToList();
        Assert.Contains(files, f => f.EndsWith("left.arrows", StringComparison.Ordinal));
        Assert.Equal(files, Directory.GetFiles(pagedDir, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(pagedDir, f)).Order(StringComparer.Ordinal));
        foreach (var f in files.Where(f => f.EndsWith(".arrows", StringComparison.Ordinal)))
        {
            Assert.Equal(File.ReadAllBytes(Path.Combine(boxedDir, f)), File.ReadAllBytes(Path.Combine(pagedDir, f)));
        }
    }
}
