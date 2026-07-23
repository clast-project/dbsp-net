// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Regression cover for the restore bug found by IvmRecoveryProbe on real SF=3 state
// (docs/design-incremental-persistence.md §7.2): a materialised view over a floating-point
// aggregate comes back at exactly 2x its row count after a snapshot restore.
//
// IncrementalAggregateOp.LoadAsync deliberately does not serialise its per-group caches; it
// rebuilds them by calling aggregator.Update(ref state, None, group, group) once per group over
// the restored trace — a BULK fold. The live run built the same cache by INCREMENTAL folds, one
// per tick. persistence.md argues these converge, and for SUM / COUNT / MIN / MAX they do, because
// those are exact. SqlAvgAggregator (AvgStateDouble) and SqlStddevAggregator (MomentState)
// accumulate `double Sum` with repeated `+=`, and floating-point addition is not associative.
//
// When the two disagree, the operator's next tick retracts the value it now holds in _aggCache
// while the downstream integrated view holds the value that was actually materialised before the
// snapshot. The retraction does not cancel, and the view keeps BOTH rows.
//
// The construction below makes the disagreement deterministic rather than relying on
// trace-enumeration order:
//
//   incremental — ten separate ticks of `Sum += 1.0` against 1e16, each absorbed  -> 1e16
//   bulk        — the trace coalesces those ten rows to (1.0, weight 10), so the reload
//                 folds `Sum += 1.0 * 10` in one operation, which is NOT absorbed
//                                                                              -> 1.000000000000001e16
//
// ulp(1e16) is 2, so a single +1.0 rounds away but a single +10.0 does not. No hash ordering,
// no platform-specific rounding mode — just the weight coalescing that the trace does by design.
using DbspNet.Core.Algebra;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

public class FloatAggregateRestoreTests : IDisposable
{
    private const double Big = 1e16;
    private const int SmallTicks = 10;

    private readonly string _snapshotDir;

    public FloatAggregateRestoreTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-floatagg-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    // Guards the premise: if this ever stops holding, the test below is no longer exercising
    // what it claims to and should be re-derived rather than deleted.
    [Fact]
    public void Premise_IncrementalAndBulkFoldsDisagree()
    {
        var incremental = Big;
        for (var i = 0; i < SmallTicks; i++)
        {
            incremental += 1.0;
        }

        var bulk = Big + (1.0 * SmallTicks);
        Assert.NotEqual(incremental, bulk);
    }

    private static CompiledProgram CompileProgram(string aggregate) =>
        SqlProgram.Compile(
            [
                "CREATE TABLE t (k INT NOT NULL, v DOUBLE NOT NULL)",
                $"CREATE VIEW agg AS SELECT k, {aggregate} AS a FROM t GROUP BY k",
            ],
            new HashSet<string>(["agg"], StringComparer.Ordinal),
            ArrowSqlSnapshotCodecs.Instance);

    // Drives the ticks that build a group whose incremental and bulk folds differ. Stops short of
    // the final delta, which is what makes the operator re-emit.
    private static void DriveUpToSnapshot(CompiledProgram p)
    {
        p.Table("t").Insert(1, Big);
        p.Step();

        // Ten SEPARATE ticks of the same row. The trace coalesces them to weight 10; the live
        // aggregate cache folds them one at a time.
        for (var i = 0; i < SmallTicks; i++)
        {
            p.Table("t").Insert(1, 1.0);
            p.Step();
        }
    }

    // The delta that forces the aggregate to retract its cached value and emit a new one.
    private static void DriveFinalDelta(CompiledProgram p)
    {
        p.Table("t").Insert(1, 2.0);
        p.Step();
    }

    private static List<(object?[] Row, long Weight)> View(CompiledProgram p)
    {
        var rows = new List<(object?[], long)>();
        foreach (var (row, weight) in p.Outputs["agg"].CurrentView)
        {
            var cells = new object?[p.Outputs["agg"].Schema.Count];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = row[i];
            }

            rows.Add((cells, weight.Value));
        }

        return rows;
    }

    private static string Describe(List<(object?[] Row, long Weight)> rows) =>
        string.Join("; ", rows
            .Select(r => "[" + string.Join(",", r.Row.Select(c => c?.ToString() ?? "null")) + "]=" + r.Weight)
            .OrderBy(s => s, StringComparer.Ordinal));

    [Theory]
    [InlineData("AVG(v)")]
    [InlineData("STDDEV(v)")]
    public async Task RestoredFloatAggregate_NextTickDoesNotDuplicateTheView(string aggregate)
    {
        // Reference: one uninterrupted run, all ticks including the final delta.
        var uninterrupted = CompileProgram(aggregate);
        DriveUpToSnapshot(uninterrupted);
        DriveFinalDelta(uninterrupted);
        var expected = View(uninterrupted);

        // A GROUP BY over one key must leave exactly one live row, whatever the value is.
        Assert.Single(expected);
        Assert.Equal(1, expected[0].Weight);

        // Recovered: the same ticks, but with a snapshot/restore boundary before the final delta.
        var producer = CompileProgram(aggregate);
        DriveUpToSnapshot(producer);
        await Snapshot.WriteAsync(producer.Circuit, _snapshotDir);

        var recovered = CompileProgram(aggregate);
        await Snapshot.ReadAsync(recovered.Circuit, _snapshotDir);

        // The restore itself must be faithful — this is the part that already passed on SF=3.
        Assert.Equal(Describe(View(producer)), Describe(View(recovered)));

        DriveFinalDelta(recovered);

        // …and the first tick after a restore must not duplicate the view. Before the fix this
        // yields two rows: the pre-snapshot value that was never retracted, plus the new one.
        var actual = View(recovered);
        Assert.True(
            actual.Count == 1 && actual[0].Weight == 1,
            $"view has {actual.Count} row(s) after restore + one delta, expected 1: {Describe(actual)}");
        Assert.Equal(Describe(expected), Describe(actual));
    }
}
