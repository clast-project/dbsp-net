// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Schema-fingerprint coverage. The plan fingerprint
/// (<c>SnapshotManifest.PlanFingerprint</c>) catches operator-type
/// drift; the schema fingerprint
/// (<c>SnapshotManifest.SchemaFingerprint</c>) catches drift in row /
/// key / value schemas that operator types don't see — VARCHAR length
/// changes, DECIMAL precision/scale changes, intermediate column
/// reorders, etc.
/// </summary>
public class SchemaDriftTests : IDisposable
{
    private readonly string _snapshotDir;

    public SchemaDriftTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-drift-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    [Fact]
    public async Task RoundTrip_SamePlanSameSchema_LoadsCleanly()
    {
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Step();

        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        // Should load without throwing.
        await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir);
    }

    [Fact]
    public async Task VarcharLengthChange_DetectedBySchemaFingerprint()
    {
        // VARCHAR length doesn't show up in operator types or in Arrow's
        // StringType — but it changes SqlType.Display, which is what the
        // schema fingerprint hashes.
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(16) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("schema fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DecimalPrecisionChange_DetectedBySchemaFingerprint()
    {
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt DECIMAL(10,2) NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");
        producer.Table("t").Insert(1, "10.50");
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt DECIMAL(12,2) NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("schema fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DecimalScaleChange_DetectedBySchemaFingerprint()
    {
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt DECIMAL(10,2) NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");
        producer.Table("t").Insert(1, "10.50");
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        // Scale change with same precision still alters the type.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt DECIMAL(10,4) NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("schema fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NullabilityChange_DetectedBySchemaFingerprint()
    {
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");
        producer.Table("t").Insert(1, 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        // amt becomes nullable — same width, same Arrow type, but
        // SqlType.Display differs. Note: nullability also flips the
        // SQL compiler from the typed fast path to the structural
        // fallback (typed-row gate rejects nullable columns), so the
        // operator graph itself changes and "plan fingerprint
        // mismatch" fires before the schema-level check is reached.
        // Either drift signal is acceptable here — both correctly
        // refuse to load the snapshot.
        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt BIGINT)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ColumnRename_DetectedBySchemaFingerprint()
    {
        // The renamed column flows into the codec's schema (column
        // names are part of SqlType.Display? No — names are separate.
        // They're hashed alongside Display in SchemaFingerprint).
        var producer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amount BIGINT NOT NULL)"],
            "SELECT id, SUM(amount) FROM t GROUP BY id");
        producer.Table("t").Insert(1, 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT id, SUM(amt) FROM t GROUP BY id");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("schema fingerprint mismatch", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OldManifestSchemaVersion_RejectedCleanly()
    {
        // Hand-craft a v1 manifest (without SchemaFingerprint) and
        // verify it's rejected with a clear "not supported" error,
        // not a fingerprint-comparison error.
        var snapDir = Path.Combine(_snapshotDir, "snap-0");
        Directory.CreateDirectory(snapDir);
        var v1Json =
            "{\n" +
            "  \"schema_version\": 1,\n" +
            "  \"plan_fingerprint\": \"00000000\",\n" +
            "  \"tick\": 0,\n" +
            "  \"operator_count\": 0,\n" +
            "  \"snapshotted_indices\": []\n" +
            "}";
        File.WriteAllText(Path.Combine(snapDir, "manifest.json"), v1Json);
        File.WriteAllText(Path.Combine(_snapshotDir, "current.txt"), "snap-0");

        var consumer = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t UNION SELECT id FROM t");

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await Snapshot.ReadAsync(consumer.Circuit, _snapshotDir));
        Assert.Contains("schema version 1 not supported", ex.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ManifestPersistsSchemaFingerprint()
    {
        var producer = Compile(
            ["CREATE TABLE sales (cat VARCHAR(8) NOT NULL, amt BIGINT NOT NULL)"],
            "SELECT cat, SUM(amt) FROM sales GROUP BY cat");
        producer.Table("sales").Insert("a", 10L);
        producer.Step();
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var snapDir = Path.Combine(_snapshotDir, "snap-" + producer.Circuit.TickCount);
        var manifest = await SnapshotManifest.ReadAsync(Path.Combine(snapDir, "manifest.json"));
        Assert.Equal(2, manifest.SchemaVersion);
        Assert.False(string.IsNullOrEmpty(manifest.SchemaFingerprint));
        Assert.Equal(16, manifest.SchemaFingerprint.Length);   // xxh3-64 hex
    }
}
