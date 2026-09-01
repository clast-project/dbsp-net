// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Arrow;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Restore loads operators concurrently (docs/design-incremental-persistence.md §11.4). The property
/// that has to hold is that the degree is invisible: snapshotted operators own disjoint state — the
/// compiler refuses to share an arrangement across joins when snapshot codecs are present — so a
/// restore at any degree must produce exactly the state a sequential one does, and must surface a
/// failing load as the same exception rather than an <see cref="AggregateException"/> wrapper.
/// </summary>
public class ParallelRestoreTests : IDisposable
{
    private readonly string _dir;

    public ParallelRestoreTests() =>
        _dir = Path.Combine(Path.GetTempPath(), "dbspnet-par-restore-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    private static readonly string[] Ddl =
    {
        "CREATE TABLE orders (id INT NOT NULL, customer INT NOT NULL, amount BIGINT NOT NULL, region VARCHAR(8) NOT NULL)",
        "CREATE TABLE customers (id INT NOT NULL, name VARCHAR(8) NOT NULL)",
    };

    // Several stateful operator kinds in one circuit, so the concurrent walk has more than one
    // shape to get wrong: a join, a grouped aggregate and a COUNT(DISTINCT).
    private const string Query =
        "SELECT orders.region, COUNT(*), SUM(orders.amount), COUNT(DISTINCT orders.customer) " +
        "FROM orders JOIN customers ON orders.customer = customers.id " +
        "GROUP BY orders.region";

    private static CompiledQuery Compile(bool codecs = true)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var d in Ddl)
        {
            resolver.Resolve(Parser.ParseStatement(d));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return codecs
            ? PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance)
            : PlanToCircuit.Compile(plan);
    }

    private static void Feed(CompiledQuery q, int from, int rows)
    {
        for (var i = from; i < from + rows; i++)
        {
            q.Table("customers").Insert(i % 7, "c" + (i % 7));
            q.Table("orders").Insert(i, i % 7, (long)(i * 3), "r" + (i % 3));
            q.Step();
        }
    }

    private static List<string> View(CompiledQuery q)
    {
        var rows = new List<string>();
        foreach (var (row, weight) in q.Current)
        {
            rows.Add(row + " x" + weight.Value);
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(16)]  // more than the operator count: every operator starts at once
    public async Task RestoreIsIdenticalAtEveryDegree(int parallelism)
    {
        var recorded = Compile();
        Feed(recorded, 0, 40);
        await Snapshot.WriteAsync(recorded.Circuit, _dir);

        var restored = Compile();
        var count = await Snapshot.ReadAsync(restored.Circuit, _dir, parallelism);
        Assert.True(count > 0);

        // State is only observable through what it makes the circuit emit, so drive both with the
        // same continuation: a restore that lost or duplicated any operator's state produces a
        // different delta here. This is the resume shape — restore, then keep stepping.
        Feed(recorded, 40, 5);
        Feed(restored, 40, 5);
        Assert.Equal(View(recorded), View(restored));
        Assert.NotEmpty(View(restored));
    }

    [Fact]
    public async Task DegreeDoesNotChangeTheOperatorCount()
    {
        var recorded = Compile();
        Feed(recorded, 0, 10);
        await Snapshot.WriteAsync(recorded.Circuit, _dir);

        var sequential = await Snapshot.ReadAsync(Compile().Circuit, _dir, 1);
        var concurrent = await Snapshot.ReadAsync(Compile().Circuit, _dir, 8);
        Assert.Equal(sequential, concurrent);
    }

    [Fact]
    public async Task AFailingLoadSurfacesUnwrapped()
    {
        var recorded = Compile();
        Feed(recorded, 0, 10);
        await Snapshot.WriteAsync(recorded.Circuit, _dir);

        // Corrupt one operator's trace so the failure happens INSIDE the concurrent walk (a
        // fingerprint mismatch would be caught before it, and would not test the unwrapping).
        var victim = Directory.EnumerateFiles(_dir, "*.arrows", SearchOption.AllDirectories).First();
        await File.WriteAllBytesAsync(victim, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });

        var thrown = await Record.ExceptionAsync(
            async () => await Snapshot.ReadAsync(Compile().Circuit, _dir, 8));

        Assert.NotNull(thrown);
        Assert.IsNotType<AggregateException>(thrown);
    }

    [Fact]
    public void DefaultDegreeIsBoundedAndAtLeastOne()
    {
        // The cap is the measured part: degree 14 was slower than 8 on a 14-core box (§11.4).
        Assert.InRange(Snapshot.DefaultRestoreParallelism, 1, 8);
    }

    [Fact]
    public async Task ProfileLoadForcesTheSequentialWalk()
    {
        var recorded = Compile();
        Feed(recorded, 0, 10);
        await Snapshot.WriteAsync(recorded.Circuit, _dir);

        Snapshot.ProfileLoad = true;
        try
        {
            var restored = Compile();
            await Snapshot.ReadAsync(restored.Circuit, _dir, 8);

            // A per-operator profile only exists if the walk really was sequential.
            Assert.NotEmpty(Snapshot.LastLoadProfile);

            Feed(recorded, 10, 3);
            Feed(restored, 10, 3);
            Assert.Equal(View(recorded), View(restored));
        }
        finally
        {
            Snapshot.ProfileLoad = false;
        }
    }
}
