using System.Text;
using DbspNet.Core.Circuit;
using DbspNet.Persistence;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Atomic-commit semantics for <see cref="Snapshot.Write"/> under the
/// versioned layout: each new snapshot is staged at
/// <c>{snapshotDir}/snap-T.tmp/</c>, atomically renamed to
/// <c>snap-T/</c>, and committed by an atomic update of
/// <c>{snapshotDir}/current.txt</c>. Crashes anywhere in the sequence
/// leave either the prior snapshot or the new one fully readable
/// (whichever <c>current.txt</c> names) — never a half-mixed state.
/// </summary>
public class AtomicSnapshotTests : IDisposable
{
    private readonly string _snapshotDir;

    public AtomicSnapshotTests()
    {
        _snapshotDir = Path.Combine(
            Path.GetTempPath(), "dbspnet-atomic-" + Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_snapshotDir))
        {
            Directory.Delete(_snapshotDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Test op: writes <see cref="Value"/> as a string. Optionally throws
    /// on Save (simulating a crash mid-write).
    /// </summary>
    private sealed class FaultableCounterOp : IOperator, ISnapshotable
    {
        public int Value { get; set; }

        public bool ThrowOnSave { get; set; }

        public void Step() => Value++;

        public void Save(ISnapshotWriter writer)
        {
            if (ThrowOnSave)
            {
                throw new InvalidOperationException("simulated crash mid-Save");
            }

            using var stream = writer.OpenWrite("value.txt");
            using var sw = new StreamWriter(stream, Encoding.UTF8);
            sw.Write(Value);
        }

        public void Load(ISnapshotReader reader)
        {
            using var stream = reader.OpenRead("value.txt");
            using var sr = new StreamReader(stream, Encoding.UTF8);
            Value = int.Parse(sr.ReadToEnd(), System.Globalization.CultureInfo.InvariantCulture);
        }

        public string SchemaFingerprint => "faultable-counter";
    }

    private static RootCircuit Build(params IOperator[] ops)
    {
        return RootCircuit.Build(builder =>
        {
            foreach (var op in ops)
            {
                builder.AddRawOperator(op);
            }
        });
    }

    [Fact]
    public void Write_OverwritingAtSameTick_PointsToLatest()
    {
        // Two writes at the same tick (Value mutates between them, but
        // circuit.TickCount stays 0 since we never Step). Both target
        // the same snap-{0}/ — second one overwrites the per-op blobs
        // and current.txt names it.
        var op = new FaultableCounterOp { Value = 1 };
        var circuit = Build(op);
        Snapshot.Write(circuit, _snapshotDir);

        op.Value = 2;
        Snapshot.Write(circuit, _snapshotDir);

        Assert.True(Snapshot.Exists(_snapshotDir));

        var consumerOp = new FaultableCounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, _snapshotDir);
        Assert.Equal(2, consumerOp.Value);
    }

    [Fact]
    public void Write_ThrowsMidSave_PriorSnapshotUntouched()
    {
        // Establish a "good" snapshot at tick 0.
        var op = new FaultableCounterOp { Value = 7 };
        var circuit = Build(op);
        Snapshot.Write(circuit, _snapshotDir);

        // Step so the next write would target snap-1 (different from
        // the existing snap-0). Then make Save throw.
        circuit.Step();
        op.ThrowOnSave = true;
        Assert.Throws<InvalidOperationException>(() => Snapshot.Write(circuit, _snapshotDir));

        // The prior good snapshot (snap-0) is still loadable: its
        // current.txt still names it.
        op.ThrowOnSave = false;
        var consumerOp = new FaultableCounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, _snapshotDir);
        // CounterOp.Value was 8 (incremented by Step) when we tried to
        // write the second snapshot. That write failed, so the loaded
        // value comes from the original snap-0 where Value was 7.
        Assert.Equal(7, consumerOp.Value);
    }

    [Fact]
    public void Write_FailedMidSave_LeavesNoVisibleSnapshot()
    {
        // Mid-Save throw with no prior snapshot: current.txt is never
        // committed, so the partial state on disk is invisible to
        // readers. (Per-blob orphans may exist as orphan keys but
        // Snapshot.Exists / Read only consult current.txt.)
        var op = new FaultableCounterOp { Value = 1, ThrowOnSave = true };
        var circuit = Build(op);
        Assert.Throws<InvalidOperationException>(() => Snapshot.Write(circuit, _snapshotDir));

        Assert.False(Snapshot.Exists(_snapshotDir));

        // Next successful write commits cleanly.
        op.ThrowOnSave = false;
        Snapshot.Write(circuit, _snapshotDir);
        Assert.True(Snapshot.Exists(_snapshotDir));

        var consumerOp = new FaultableCounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, _snapshotDir);
        Assert.Equal(1, consumerOp.Value);
    }

    [Fact]
    public void Read_StaleCurrentPointer_ThrowsFileNotFound()
    {
        // Manually point current.txt at a snap-T that doesn't exist —
        // simulates a corrupt or hand-edited pointer. Read should
        // surface a clear "missing manifest" error rather than silently
        // proceeding.
        Directory.CreateDirectory(_snapshotDir);
        File.WriteAllText(Path.Combine(_snapshotDir, "current.txt"), "snap-999");

        var consumer = Build(new FaultableCounterOp());
        Assert.Throws<FileNotFoundException>(() => Snapshot.Read(consumer, _snapshotDir));
    }

    [Fact]
    public void Exists_ReturnsTrue_AfterSuccessfulWrite()
    {
        var op = new FaultableCounterOp { Value = 1 };
        var circuit = Build(op);
        Snapshot.Write(circuit, _snapshotDir);
        Assert.True(Snapshot.Exists(_snapshotDir));
    }

    [Fact]
    public void Exists_ReturnsFalse_WhenDirectoryDoesNotExist()
    {
        Assert.False(Snapshot.Exists(_snapshotDir));
    }

    [Fact]
    public void Exists_ReturnsFalse_WhenCurrentTxtMissing()
    {
        // Simulate orphan snap-T from a crash before current.txt was
        // committed. Without current.txt, no snapshot exists from the
        // reader's perspective.
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "snap-0"));
        File.WriteAllText(
            Path.Combine(_snapshotDir, "snap-0", "manifest.json"),
            "{}");

        Assert.False(Snapshot.Exists(_snapshotDir));
    }

    [Fact]
    public void Write_OrphanSnapDir_DoesNotAffectCurrentSnapshot()
    {
        // Establish a real snapshot at snap-0.
        var op = new FaultableCounterOp { Value = 5 };
        var circuit = Build(op);
        Snapshot.Write(circuit, _snapshotDir);

        // Drop a fake orphan snap-99 alongside (simulating a crash
        // between snap-T rename and current.txt commit). Read should
        // ignore it because current.txt names snap-0.
        Directory.CreateDirectory(Path.Combine(_snapshotDir, "snap-99"));

        var consumerOp = new FaultableCounterOp();
        var consumer = Build(consumerOp);
        Snapshot.Read(consumer, _snapshotDir);
        Assert.Equal(5, consumerOp.Value);
    }
}
