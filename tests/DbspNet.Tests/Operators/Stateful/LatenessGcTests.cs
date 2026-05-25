// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// Phase-1 coverage for frontier-driven trace GC (the LATENESS mechanism) on
/// the flat <see cref="IncrementalAggregateOp{TKey,TValue,TOut}"/>. Uses a
/// logical-time <c>long</c> group key (the BIGINT bring-up carrier) so the
/// mechanism is exercised without any SQL/temporal surface. Two properties:
/// GC keeps retained state bounded on a monotone stream, and GC is observably
/// invisible — output is identical to an otherwise-equal operator with no
/// frontier.
/// </summary>
public class LatenessGcTests
{
    private sealed record Event(long Time, long Value);

    private sealed class Harness
    {
        public required RootCircuit Circuit { get; init; }
        public required InputHandle<ZSet<Event, Z64>> Input { get; init; }
        public required OutputHandle<ZSet<(long Key, long Value), Z64>> Output { get; init; }
        public required IncrementalAggregateOp<long, long, long> Op { get; init; }
    }

    // SUM(Value) GROUP BY Time. When frontier is supplied, the op GCs groups
    // whose Time is strictly below the frontier.
    private static Harness Build(MutableFrontier? frontier)
    {
        InputHandle<ZSet<Event, Z64>>? ih = null;
        OutputHandle<ZSet<(long, long), Z64>>? oh = null;
        var circuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, e => e.Time, e => e.Value);
            var aggregated = frontier is null
                ? b.IncrementalAggregate(grouped, new SumAggregator<long>())
                : b.IncrementalAggregate(
                    grouped, new SumAggregator<long>(), snapshotCodec: null,
                    frontier: frontier, monotoneKey: k => k);
            oh = b.Output(aggregated);
        });

        var op = circuit.Operators.OfType<IncrementalAggregateOp<long, long, long>>().Single();
        return new Harness { Circuit = circuit, Input = ih!, Output = oh!, Op = op };
    }

    private static ZSet<Event, Z64> Delta(params (long Time, long Value, long Weight)[] rows) =>
        ZSet.FromEntries(rows.Select(r => (new Event(r.Time, r.Value), new Z64(r.Weight))));

    [Fact]
    public void Gc_KeepsRetainedGroupsBounded_OnMonotoneStream()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        var h = Build(frontier);

        const long Last = 200;
        for (long t = 0; t <= Last; t++)
        {
            h.Input.Push(Delta((t, 1, 1)));
            frontier.AdvanceTo(t - Lateness); // maxSeen (= t) − lateness
            h.Circuit.Step();
        }

        // 201 distinct group keys streamed through, but after the final tick
        // only keys in [Last−Lateness, Last] survive — a fixed window of 11 —
        // regardless of how long the stream ran.
        Assert.Equal((int)Lateness + 1, h.Op.RetainedGroupCount);
    }

    [Fact]
    public void Gc_OutputIdenticalToNoFrontierTwin_IncludingInWindowUpdate()
    {
        const long Lateness = 10;
        var frontier = new MutableFrontier();
        var gc = Build(frontier);
        var plain = Build(frontier: null);
        long maxSeen = long.MinValue;

        void Tick(params (long Time, long Value, long Weight)[] rows)
        {
            // Fresh Z-set per circuit — InputHandle.Push merges into a pending
            // delta, so the two circuits must not alias the same instance.
            gc.Input.Push(Delta(rows));
            plain.Input.Push(Delta(rows));
            foreach (var r in rows)
            {
                maxSeen = System.Math.Max(maxSeen, r.Time);
            }

            frontier.AdvanceTo(maxSeen - Lateness);
            gc.Circuit.Step();
            plain.Circuit.Step();

            // GC reduces state, never output: the two outputs must agree tick
            // for tick.
            Assert.Equal(plain.Output.Current, gc.Output.Current);
        }

        for (long t = 0; t <= 20; t++)
        {
            Tick((t, 1, 1));
        }

        // Second row for a key still inside the lateness window (frontier = 10,
        // key 18 ≥ 10): both ops must retract (18,1) and emit (18,101).
        Tick((18, 100, 1));
        Assert.False(gc.Output.Current.IsEmpty); // sanity: the update did emit
        Assert.Equal(new Z64(-1), gc.Output.Current.WeightOf((18L, 1L)));
        Assert.Equal(Z64.One, gc.Output.Current.WeightOf((18L, 101L)));

        for (long t = 21; t <= 40; t++)
        {
            Tick((t, 1, 1));
        }

        // And the GC twin really did bound its state while staying identical.
        Assert.True(gc.Op.RetainedGroupCount <= (int)Lateness + 1);
        Assert.True(plain.Op.RetainedGroupCount > (int)Lateness + 1);
    }

    [Fact]
    public void Gc_WithoutFrontierAdvance_RetainsEverything()
    {
        // Frontier supplied but never advanced (stays long.MinValue): GC must
        // be inert, so the op is opt-in and safe by default.
        var frontier = new MutableFrontier();
        var h = Build(frontier);

        for (long t = 0; t < 50; t++)
        {
            h.Input.Push(Delta((t, 1, 1)));
            h.Circuit.Step();
        }

        Assert.Equal(50, h.Op.RetainedGroupCount);
    }
}
