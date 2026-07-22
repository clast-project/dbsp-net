// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// A1 of docs/design-incremental-persistence.md: WalRecorder was hard-coupled to
// CompiledQuery, so the program path — the one the ivm-bench server drives — could not
// reach approach (A)/(C) at all. It now works over ICompiledCircuit, which both compiled
// shapes implement. These tests exercise the program side of that: the same WAL
// machinery the query tests cover (WalRecorderTests / HybridSnapshotWalTests), but
// driven through a multi-table, multi-view CompiledProgram.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class ProgramWalRecorderTests : IDisposable
{
    private readonly string _walDir;
    private readonly string _snapshotDir;

    public ProgramWalRecorderTests()
    {
        var id = Guid.NewGuid().ToString("N");
        _walDir = Path.Combine(Path.GetTempPath(), "dbspnet-progwal-" + id);
        _snapshotDir = Path.Combine(Path.GetTempPath(), "dbspnet-progsnap-" + id);
    }

    public void Dispose()
    {
        foreach (var dir in new[] { _walDir, _snapshotDir })
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }

        GC.SuppressFinalize(this);
    }

    // Two source tables and a view DAG over both — the shape a CompiledQuery cannot
    // express, and the reason the coupling mattered.
    private static readonly string[] ProgramSql =
    [
        "CREATE TABLE orders (id INT NOT NULL, cust INT NOT NULL, amount INT NOT NULL)",
        "CREATE TABLE customers (cust INT NOT NULL, region VARCHAR NOT NULL)",
        "CREATE VIEW enriched AS SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.cust",
        "CREATE VIEW by_region AS SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM enriched GROUP BY region",
        "CREATE VIEW regions AS SELECT DISTINCT region FROM customers",
    ];

    private static readonly HashSet<string> OutputViews =
        new(["by_region", "regions"], StringComparer.Ordinal);

    private static CompiledProgram Compile() =>
        SqlProgram.Compile(ProgramSql, OutputViews, ArrowSqlSnapshotCodecs.Instance);

    // Stable string form of an output view, so two programs can be compared without
    // depending on Z-set enumeration order.
    private static string Dump(CompiledProgram program, string view)
    {
        var output = program.Outputs[view];
        var rows = new List<string>();
        foreach (var (row, weight) in output.CurrentView)
        {
            var cells = new string[output.Schema.Count];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = row[i]?.ToString() ?? "<null>";
            }

            rows.Add(string.Join("|", cells) + " => " + weight.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        rows.Sort(StringComparer.Ordinal);
        return string.Join("\n", rows);
    }

    private static string DumpAll(CompiledProgram program) =>
        string.Join("\n--\n", OutputViews.OrderBy(v => v, StringComparer.Ordinal).Select(v => Dump(program, v)));

    // Four ticks of input across both tables, including a retraction, so the replayed
    // state is not reachable by simply re-inserting everything.
    private static void DriveTick(CompiledProgram p, int tick)
    {
        switch (tick)
        {
            case 0:
                p.Table("customers").Insert(1, "west");
                p.Table("customers").Insert(2, "east");
                p.Table("orders").Insert(10, 1, 100);
                break;
            case 1:
                p.Table("orders").Insert(11, 1, 50);
                p.Table("orders").Insert(12, 2, 70);
                break;
            case 2:
                p.Table("orders").Delete(10, 1, 100);
                p.Table("customers").Insert(3, "west");
                break;
            case 3:
                p.Table("orders").Insert(13, 3, 25);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(tick));
        }
    }

    [Fact]
    public async Task Program_RecordsManifestAndPerTableSegments()
    {
        var program = Compile();
        await using (var wal = await WalRecorder.CreateAsync(program, _walDir))
        {
            DriveTick(program, 0);
            await wal.StepAsync();
            DriveTick(program, 1);
            await wal.StepAsync();
        }

        Assert.True(File.Exists(Path.Combine(_walDir, "manifest.json")));
        // One segment file per SOURCE table — views are derived, never logged.
        Assert.True(File.Exists(Path.Combine(_walDir, "orders.0.arrows")));
        Assert.True(File.Exists(Path.Combine(_walDir, "customers.0.arrows")));
        Assert.False(File.Exists(Path.Combine(_walDir, "enriched.0.arrows")));

        var manifest = await WalManifest.ReadAsync(Path.Combine(_walDir, "manifest.json"));
        Assert.Equal(["customers", "orders"], manifest.Tables.OrderBy(t => t, StringComparer.Ordinal));
        Assert.Single(manifest.Segments);
        Assert.Equal(2, manifest.Segments[0].Ticks);
    }

    [Fact]
    public async Task Program_ReplayRestoresEveryOutputView()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir))
        {
            for (var t = 0; t < 4; t++)
            {
                DriveTick(producer, t);
                await wal.StepAsync();
            }
        }

        var expected = DumpAll(producer);
        Assert.NotEmpty(expected);

        // A fresh program replays the WAL and must land on the same state — every
        // output view, not just the one a CompiledQuery would have had.
        var replayed = Compile();
        await using (await WalRecorder.CreateAsync(replayed, _walDir))
        {
            Assert.Equal(expected, DumpAll(replayed));
            Assert.Equal(producer.Circuit.TickCount, replayed.Circuit.TickCount);
        }
    }

    [Fact]
    public async Task Program_SnapshotPlusWal_ReplaysOnlyTicksPastTheSnapshot()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir))
        {
            DriveTick(producer, 0);
            await wal.StepAsync();
            DriveTick(producer, 1);
            await wal.StepAsync();

            // Snapshot at tick 2; the segments below it are pruned and a fresh
            // segment opens for the remaining ticks.
            await wal.WriteSnapshotAsync();

            DriveTick(producer, 2);
            await wal.StepAsync();
            DriveTick(producer, 3);
            await wal.StepAsync();
        }

        var expected = DumpAll(producer);

        var recovered = Compile();
        await using (await WalRecorder.CreateAsync(recovered, _walDir, _snapshotDir))
        {
            Assert.Equal(expected, DumpAll(recovered));
            Assert.Equal(producer.Circuit.TickCount, recovered.Circuit.TickCount);
        }
    }

    [Fact]
    public async Task Program_SnapshotOnly_RecoversWithoutAnyWalTicksToReplay()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir, _snapshotDir))
        {
            for (var t = 0; t < 4; t++)
            {
                DriveTick(producer, t);
                await wal.StepAsync();
            }

            await wal.WriteSnapshotAsync();
        }

        var expected = DumpAll(producer);

        var recovered = Compile();
        await using (await WalRecorder.CreateAsync(recovered, _walDir, _snapshotDir))
        {
            Assert.Equal(expected, DumpAll(recovered));
            Assert.Equal(producer.Circuit.TickCount, recovered.Circuit.TickCount);
        }
    }

    [Fact]
    public async Task Program_InputSchemaDrift_IsRefused()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir))
        {
            DriveTick(producer, 0);
            await wal.StepAsync();
        }

        // Same views, but `amount` widens to BIGINT — an input-schema change, which is
        // exactly what the WAL fingerprint exists to catch.
        var drifted = SqlProgram.Compile(
            [
                "CREATE TABLE orders (id INT NOT NULL, cust INT NOT NULL, amount BIGINT NOT NULL)",
                "CREATE TABLE customers (cust INT NOT NULL, region VARCHAR NOT NULL)",
                "CREATE VIEW enriched AS SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.cust",
                "CREATE VIEW by_region AS SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM enriched GROUP BY region",
                "CREATE VIEW regions AS SELECT DISTINCT region FROM customers",
            ],
            OutputViews,
            ArrowSqlSnapshotCodecs.Instance);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await WalRecorder.CreateAsync(drifted, _walDir));
    }

    [Fact]
    public async Task Program_ViewBodyChange_DoesNotInvalidateTheWal()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir))
        {
            for (var t = 0; t < 4; t++)
            {
                DriveTick(producer, t);
                await wal.StepAsync();
            }
        }

        // The fingerprint covers input table schemas only, so refactoring a view body
        // must still replay — the property the query path already relies on.
        var refactored = SqlProgram.Compile(
            [
                "CREATE TABLE orders (id INT NOT NULL, cust INT NOT NULL, amount INT NOT NULL)",
                "CREATE TABLE customers (cust INT NOT NULL, region VARCHAR NOT NULL)",
                "CREATE VIEW enriched AS SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.cust",
                "CREATE VIEW by_region AS SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM enriched GROUP BY region",
                "CREATE VIEW regions AS SELECT DISTINCT region FROM customers WHERE cust = 2",
            ],
            OutputViews,
            ArrowSqlSnapshotCodecs.Instance);

        await using (await WalRecorder.CreateAsync(refactored, _walDir))
        {
            Assert.Equal(4, refactored.Circuit.TickCount);
            // The original `regions` is {east, west}; the refactored one filters to
            // cust=2 alone. Getting {east} proves the replay drove the NEW plan rather
            // than restoring the old one's state.
            Assert.Equal("east => 1", Dump(refactored, "regions"));
            Assert.Equal("east => 1\nwest => 1", Dump(producer, "regions"));
        }
    }

    [Fact]
    public async Task Program_TableSetMismatch_IsRefused()
    {
        var producer = Compile();
        await using (var wal = await WalRecorder.CreateAsync(producer, _walDir))
        {
            DriveTick(producer, 0);
            await wal.StepAsync();
        }

        // A program with an extra source table has a different WAL table set.
        var extraTable = SqlProgram.Compile(
            [
                "CREATE TABLE orders (id INT NOT NULL, cust INT NOT NULL, amount INT NOT NULL)",
                "CREATE TABLE customers (cust INT NOT NULL, region VARCHAR NOT NULL)",
                "CREATE TABLE promos (code VARCHAR NOT NULL)",
                "CREATE VIEW enriched AS SELECT o.id, o.amount, c.region FROM orders o JOIN customers c ON o.cust = c.cust",
                "CREATE VIEW by_region AS SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM enriched GROUP BY region",
                "CREATE VIEW regions AS SELECT DISTINCT region FROM customers",
                "CREATE VIEW codes AS SELECT DISTINCT code FROM promos",
            ],
            new HashSet<string>(["by_region", "regions", "codes"], StringComparer.Ordinal),
            ArrowSqlSnapshotCodecs.Instance);

        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await WalRecorder.CreateAsync(extraTable, _walDir));
    }
}
