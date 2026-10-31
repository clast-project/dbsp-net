// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The typed-path sibling of FloatAggregateRestoreTests (docs/design-incremental-persistence.md §7.2/§8).
//
// §8 fixed the order-sensitive float accumulators on the STRUCTURAL aggregate by persisting their
// state per slot. No Typed* aggregator implements CanPersistState, so the typed trace-backed aggregate
// (TypedCompositeAggregator, used when the aggregate list is not all-linear — e.g. AVG mixed with MIN)
// restores by folding the trace: the reconstruction §7.2 showed to be wrong. Same deterministic
// construction as the structural test — ten +1.0 ticks absorbed by 1e16 incrementally, but not when
// the trace coalesces them to one weight-10 row.
using DbspNet.Core.Algebra;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Persistence;

public sealed class TypedFloatAggregateRestoreTests : IDisposable
{
    private const double Big = 1e16;
    private const int SmallTicks = 10;

    private readonly string _dir = Path.Combine(
        Path.GetTempPath(), "dbspnet-typed-floatagg-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    private static CompiledQuery Compile(bool nullable, string query)
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(
            $"CREATE TABLE t (k INT NOT NULL, v DOUBLE{(nullable ? "" : " NOT NULL")})"));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void DriveUpToSnapshot(CompiledQuery q)
    {
        q.Table("t").Insert(1, Big);
        q.Step();
        for (var i = 0; i < SmallTicks; i++)
        {
            q.Table("t").Insert(1, 1.0);
            q.Step();
        }
    }

    private static List<string> Delta(CompiledQuery q) => q.Current
        .Select(e => string.Join("|", Enumerable.Range(0, e.Key.Count)
            .Select(i => FormattableString.Invariant($"{e.Key[i]:R}"))) + "#" + e.Value.Value)
        .OrderBy(x => x, StringComparer.Ordinal)
        .ToList();

    // MIN alongside the float aggregate keeps the composite non-linear, so the typed path builds
    // the trace-backed aggregate rather than the trace-free linear one (which persists its state).
    // The nullable column selects the *Nullable aggregator variants.
    [Theory]
    [InlineData(false, "SELECT k, AVG(v) AS a, MIN(v) AS m FROM t GROUP BY k")]
    [InlineData(false, "SELECT k, SUM(v) AS a, MAX(v) AS m FROM t GROUP BY k")]
    [InlineData(true, "SELECT k, AVG(v) AS a, MIN(v) AS m FROM t GROUP BY k")]
    [InlineData(true, "SELECT k, SUM(v) AS a, MAX(v) AS m FROM t GROUP BY k")]
    public async Task TypedRestoredFloatAggregate_NextTickMatchesUninterruptedRun(bool nullable, string query)
    {
        var reference = Compile(nullable, query);
        Assert.Contains(reference.Circuit.Operators,
            o => o.GetType().FullName!.Contains("TypedRow_", StringComparison.Ordinal));
        DriveUpToSnapshot(reference);

        var producer = Compile(nullable, query);
        DriveUpToSnapshot(producer);
        await Snapshot.WriteAsync(producer.Circuit, _dir);

        var restored = Compile(nullable, query);
        await Snapshot.ReadAsync(restored.Circuit, _dir);

        reference.Table("t").Insert(1, 2.0);
        restored.Table("t").Insert(1, 2.0);
        reference.Step();
        restored.Step();

        // Bit-exact (round-trip formatting): the restored aggregate must retract exactly the value
        // the uninterrupted run would, or a downstream integral keeps both rows.
        Assert.Equal(Delta(reference), Delta(restored));
    }
}
