// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Establishes, as evidence for docs/design-durable-identity.md, exactly which plan changes a
// checkpoint survives today.
//
// Snapshot state is addressed POSITIONALLY: the on-disk key is `op-{i}`, where i is the operator's
// index in RootCircuit.Operators build order, and restore is guarded by a plan fingerprint plus an
// operator count. Nobody has written down what that implies for a redeploy, so these tests pin it:
// a checkpoint is portable across a recompile of the SAME program, and NOT across a program that
// gained an unrelated view — even though every operator the checkpoint covers is unchanged.
//
// The guard is fail-safe (it throws rather than mis-mapping), so this is a capability limit rather
// than a defect. Whether it should be lifted is the question the design doc has to answer.
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class OperatorIdentityProbeTests : IDisposable
{
    private readonly string _dir;

    public OperatorIdentityProbeTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "dbspnet-identity-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    // `extraOutput` must be a DESIGNATED OUTPUT, not merely a declared view: a view that no
    // output depends on is pruned and never reaches the circuit at all, so adding one changes
    // nothing. (That is itself worth knowing — the first version of this test asserted a rejection
    // and got none, because the extra view had been eliminated.)
    private static CompiledProgram Compile(bool withExtraOutput)
    {
        var stmts = new List<string>
        {
            "CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)",
            "CREATE VIEW totals AS SELECT k, SUM(v) AS s FROM t GROUP BY k",
        };
        var outputs = new HashSet<string>(["totals"], StringComparer.Ordinal);
        if (withExtraOutput)
        {
            stmts.Add("CREATE VIEW unrelated AS SELECT DISTINCT k FROM t");
            outputs.Add("unrelated");
        }

        return SqlProgram.Compile(stmts, outputs, ArrowSqlSnapshotCodecs.Instance);
    }

    private static void Drive(CompiledProgram p)
    {
        p.Table("t").Insert(1, 10L);
        p.Table("t").Insert(2, 20L);
        p.Step();
    }

    [Fact]
    public async Task Checkpoint_SurvivesRecompileOfTheSameProgram()
    {
        // The baseline everything else depends on: compilation is deterministic, so the same SQL
        // produces the same operator order and the same positional keys.
        var producer = Compile(withExtraOutput: false);
        Drive(producer);
        await Snapshot.WriteAsync(producer.Circuit, _dir);

        var consumer = Compile(withExtraOutput: false);
        await Snapshot.ReadAsync(consumer.Circuit, _dir);

        Assert.Equal(producer.Circuit.TickCount, consumer.Circuit.TickCount);
        Assert.Equal(
            producer.Outputs["totals"].CurrentView.Count,
            consumer.Outputs["totals"].CurrentView.Count);
    }

    [Fact]
    public async Task Checkpoint_IsRejected_WhenAnUnrelatedViewIsAdded()
    {
        // `totals` is untouched — same SQL, same operators, same state. But adding any view shifts
        // operator indices and changes the operator count, so the positional scheme cannot tell
        // "the same operator moved" from "a different operator is here now" and refuses the load.
        var producer = Compile(withExtraOutput: false);
        Drive(producer);
        await Snapshot.WriteAsync(producer.Circuit, _dir);

        var grown = Compile(withExtraOutput: true);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(
            () => Snapshot.ReadAsync(grown.Circuit, _dir).AsTask());

        // Fail-safe, not silent: the state is never mapped onto the wrong operator.
        Assert.Contains("fingerprint", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Checkpoint_IsRejected_WhenAnUnrelatedViewIsRemoved()
    {
        // The symmetric case, so the limit is recorded in both directions.
        var producer = Compile(withExtraOutput: true);
        Drive(producer);
        await Snapshot.WriteAsync(producer.Circuit, _dir);

        var shrunk = Compile(withExtraOutput: false);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => Snapshot.ReadAsync(shrunk.Circuit, _dir).AsTask());
    }
}
