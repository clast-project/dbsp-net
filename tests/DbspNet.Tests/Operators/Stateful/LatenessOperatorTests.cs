// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// Phase-2 coverage for the input-side frontier source
/// (<see cref="LatenessOperator{TRow}"/>): late-row drop against the advancing
/// frontier, the frontier driving downstream GC (so a late row cannot resurrect
/// a collected group), and persistence of <c>maxSeen</c> across a snapshot.
/// </summary>
public class LatenessOperatorTests
{
    private sealed record Event(long Time, long Value);

    private static ZSet<Event, Z64> Delta(params (long Time, long Value, long Weight)[] rows) =>
        ZSet.FromEntries(rows.Select(r => (new Event(r.Time, r.Value), new Z64(r.Weight))));

    private static (RootCircuit Circuit, InputHandle<ZSet<Event, Z64>> Input,
        OutputHandle<ZSet<Event, Z64>> Output, LatenessOperator<Event> Op)
        BuildLateness(MutableFrontier frontier, long lateness)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<Event, Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            oh = b.Output(b.EnforceLateness(s, e => e.Time, lateness, frontier));
        });

        var op = circuit.Operators.OfType<LatenessOperator<Event>>().Single();
        return (circuit, ih!, oh!, op);
    }

    [Fact]
    public void DropsRowsBelowFrontier_AndAdvancesFrontier()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        var h = BuildLateness(frontier, Lateness);

        // Tick 0: nothing to drop (no frontier yet); maxSeen=100 → frontier 90.
        h.Input.Push(Delta((100, 1, 1)));
        h.Circuit.Step();
        Assert.Equal(Z64.One, h.Output.Current.WeightOf(new Event(100, 1)));
        Assert.Equal(0, h.Op.DroppedCount);
        Assert.Equal(90, frontier.Value);

        // Tick 1: 95 ≥ 90 kept; 80 < 90 dropped. maxSeen unchanged → frontier 90.
        h.Input.Push(Delta((95, 1, 1), (80, 1, 1)));
        h.Circuit.Step();
        Assert.Equal(1, h.Output.Current.Count);
        Assert.Equal(Z64.One, h.Output.Current.WeightOf(new Event(95, 1)));
        Assert.Equal(1, h.Op.DroppedCount);
        Assert.Equal(90, frontier.Value);

        // Tick 2: 200 advances maxSeen → frontier 190.
        h.Input.Push(Delta((200, 1, 1)));
        h.Circuit.Step();
        Assert.Equal(190, frontier.Value);

        // Tick 3: 185 < 190 dropped; 195 kept.
        h.Input.Push(Delta((185, 1, 1), (195, 1, 1)));
        h.Circuit.Step();
        Assert.Equal(1, h.Output.Current.Count);
        Assert.Equal(Z64.One, h.Output.Current.WeightOf(new Event(195, 1)));
        Assert.Equal(2, h.Op.DroppedCount);
    }

    [Fact]
    public void FrontierDrivesGc_AndLateRowCannotResurrectCollectedGroup()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var filtered = b.EnforceLateness(s, e => e.Time, Lateness, frontier);
            var grouped = b.GroupProject(filtered, e => e.Time, e => e.Value);
            oh = b.Output(b.IncrementalAggregate(
                grouped, new SumAggregator<long>(), snapshotCodec: null,
                frontier: frontier, monotoneKey: k => k));
        });
        var agg = circuit.Operators.OfType<IncrementalAggregateOp<long, long, long>>().Single();
        var lateness = circuit.Operators.OfType<LatenessOperator<Event>>().Single();

        for (long t = 0; t <= 30; t++)
        {
            ih!.Push(Delta((t, 1, 1)));
            circuit.Step();
        }

        // frontier = 30 − 10 = 20; group keys [20, 30] retained = 11.
        Assert.Equal((int)Lateness + 1, agg.RetainedGroupCount);

        // A late row for key 5 (already collected, 5 < frontier 20): dropped at
        // the input, so the aggregate sees nothing and the group stays gone.
        ih!.Push(Delta((5, 100, 1)));
        circuit.Step();
        Assert.True(oh!.Current.IsEmpty);
        Assert.Equal(1, lateness.DroppedCount);
        Assert.Equal((int)Lateness + 1, agg.RetainedGroupCount);
    }

    private sealed class PrefixedContext : ISnapshotWriter, ISnapshotReader
    {
        private readonly ITableFileSystem _fs;
        private readonly string _prefix;

        public PrefixedContext(ITableFileSystem fs, string prefix)
        {
            _fs = fs;
            _prefix = prefix;
        }

        public ValueTask<ISequentialFile> CreateAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.CreateAsync(_prefix + filename, overwrite: true, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.OpenReadAsync(_prefix + filename, cancellationToken);

        public ValueTask<bool> ExistsAsync(string filename, CancellationToken cancellationToken = default)
            => _fs.ExistsAsync(_prefix + filename, cancellationToken);
    }

    [Fact]
    public async Task MaxSeen_SnapshotRoundTrip_RestoresFrontier()
    {
        const long Lateness = 10;
        var fs = new InMemoryTableFileSystem();

        // Producer reaches maxSeen=100 → frontier 90, then snapshots.
        var producerFrontier = new MutableFrontier();
        var producer = BuildLateness(producerFrontier, Lateness);
        producer.Input.Push(Delta((100, 1, 1)));
        producer.Circuit.Step();
        Assert.Equal(90, producerFrontier.Value);
        await producer.Op.SaveAsync(new PrefixedContext(fs, "op/"));

        // A fresh op restores maxSeen and re-advances its frontier to 90.
        var consumerFrontier = new MutableFrontier();
        var consumer = BuildLateness(consumerFrontier, Lateness);
        await consumer.Op.LoadAsync(new PrefixedContext(fs, "op/"));
        Assert.Equal(90, consumerFrontier.Value);

        // So the late-drop resumes consistently: 85 < 90 dropped, 120 kept.
        consumer.Input.Push(Delta((85, 1, 1), (120, 1, 1)));
        consumer.Circuit.Step();
        Assert.Equal(1, consumer.Op.DroppedCount);
        Assert.Equal(1, consumer.Output.Current.Count);
        Assert.Equal(Z64.One, consumer.Output.Current.WeightOf(new Event(120, 1)));
    }
}
