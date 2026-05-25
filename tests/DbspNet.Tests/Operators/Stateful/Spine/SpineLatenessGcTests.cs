// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Phase-1 frontier-driven GC on the spine-backed
/// <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}"/>. Same properties
/// as the flat <c>LatenessGcTests</c> (bounded state, GC invisible to output,
/// inert without a frontier), plus a tick-for-tick cross-check that the spine
/// GC path agrees with the flat GC path.
/// </summary>
public class SpineLatenessGcTests
{
    private sealed record Event(long Time, long Value);

    private static ZSet<Event, Z64> Delta(params (long Time, long Value, long Weight)[] rows) =>
        ZSet.FromEntries(rows.Select(r => (new Event(r.Time, r.Value), new Z64(r.Weight))));

    private static (RootCircuit Circuit, InputHandle<ZSet<Event, Z64>> Input,
        OutputHandle<ZSet<(long Key, long Value), Z64>> Output,
        SpineIncrementalAggregateOp<long, long, long> Op)
        BuildSpine(MutableFrontier? frontier)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = frontier is null
                ? b.SpineIncrementalAggregate(grouped, new SumAggregator<long>())
                : b.SpineIncrementalAggregate(
                    grouped, new SumAggregator<long>(), frontier: frontier, monotoneKey: k => k);
            oh = b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<SpineIncrementalAggregateOp<long, long, long>>().Single();
        return (circuit, ih!, oh!, op);
    }

    private static (RootCircuit Circuit, InputHandle<ZSet<Event, Z64>> Input,
        OutputHandle<ZSet<(long Key, long Value), Z64>> Output,
        IncrementalAggregateOp<long, long, long> Op)
        BuildFlat(MutableFrontier frontier)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = b.IncrementalAggregate(
                grouped, new SumAggregator<long>(), snapshotCodec: null,
                frontier: frontier, monotoneKey: k => k);
            oh = b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<IncrementalAggregateOp<long, long, long>>().Single();
        return (circuit, ih!, oh!, op);
    }

    [Fact]
    public void SpineGc_KeepsRetainedGroupsBounded_OnMonotoneStream()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        var s = BuildSpine(frontier);

        const long Last = 200;
        for (long t = 0; t <= Last; t++)
        {
            s.Input.Push(Delta((t, 1, 1)));
            frontier.AdvanceTo(t - Lateness);
            s.Circuit.Step();
        }

        Assert.Equal((int)Lateness + 1, s.Op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_OutputMatchesFlatGc_IncludingInWindowUpdate()
    {
        const long Lateness = 10;
        var spineFrontier = new MutableFrontier();
        var flatFrontier = new MutableFrontier();
        var spine = BuildSpine(spineFrontier);
        var flat = BuildFlat(flatFrontier);
        long maxSeen = long.MinValue;

        void Tick(params (long Time, long Value, long Weight)[] rows)
        {
            spine.Input.Push(Delta(rows));
            flat.Input.Push(Delta(rows));
            foreach (var r in rows)
            {
                maxSeen = System.Math.Max(maxSeen, r.Time);
            }

            spineFrontier.AdvanceTo(maxSeen - Lateness);
            flatFrontier.AdvanceTo(maxSeen - Lateness);
            spine.Circuit.Step();
            flat.Circuit.Step();

            // The spine GC path must agree with the flat GC path tick for tick.
            Assert.Equal(flat.Output.Current, spine.Output.Current);
        }

        for (long t = 0; t <= 20; t++)
        {
            Tick((t, 1, 1));
        }

        // In-window update (frontier = 10, key 18 ≥ 10): retract (18,1), emit (18,101).
        Tick((18, 100, 1));
        Assert.Equal(new Z64(-1), spine.Output.Current.WeightOf((18L, 1L)));
        Assert.Equal(Z64.One, spine.Output.Current.WeightOf((18L, 101L)));

        for (long t = 21; t <= 40; t++)
        {
            Tick((t, 1, 1));
        }

        Assert.True(spine.Op.RetainedGroupCount <= (int)Lateness + 1);
        Assert.Equal(flat.Op.RetainedGroupCount, spine.Op.RetainedGroupCount);
    }

    [Fact]
    public void SpineGc_WithoutFrontierAdvance_RetainsEverything()
    {
        var frontier = new MutableFrontier();
        var s = BuildSpine(frontier);

        for (long t = 0; t < 50; t++)
        {
            s.Input.Push(Delta((t, 1, 1)));
            s.Circuit.Step();
        }

        Assert.Equal(50, s.Op.RetainedGroupCount);
    }
}
