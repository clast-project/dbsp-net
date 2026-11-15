// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using System.Linq;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// The parallel LATENESS frontier on a raw <see cref="ParallelCircuit"/>
/// (docs/design-parallel-frontier.md): each replica's <see cref="LatenessOperator{TRow}"/> joins a
/// step-boundary global-max reduction, so a row is dropped against the max over <em>all</em> shards
/// as of the start of the tick — exactly the serial rule — and every replica's frontier ends each
/// step at the serial value.
/// </summary>
public class ParallelLatenessOperatorTests
{
    private sealed record Event(long Time, long Id);

    private static ZSet<Event, Z64> Delta(IEnumerable<Event> rows) =>
        ZSet.FromEntries(rows.Select(r => (r, Z64.One)));

    private sealed class Parallel : IDisposable
    {
        public Parallel(int workers, long lateness)
        {
            var frontiers = new List<MutableFrontier>();
            Circuit = ParallelCircuit.Build(workers, b =>
            {
                var (_, s) = b.ZSetInput<Event, Z64>("in");
                var frontier = new MutableFrontier();
                frontiers.Add(frontier);
                b.Output(b.EnforceLateness(s, e => e.Time, lateness, frontier), "out");
            });
            Frontiers = frontiers;

            // Shard by Id, not by time, so the shards' local maxima differ.
            Input = Circuit.ShardedInput<Event, Z64>("in", e => (int)e.Id);
            Output = Circuit.ShardedOutput<Event, Z64>("out");
        }

        public ParallelCircuit Circuit { get; }

        public IReadOnlyList<MutableFrontier> Frontiers { get; }

        public ShardedInputHandle<Event, Z64> Input { get; }

        public ShardedOutputHandle<Event, Z64> Output { get; }

        public void Dispose() => Circuit.Dispose();
    }

    [Fact]
    public void LateRowOnAnotherShard_IsDroppedAgainstTheGlobalMax()
    {
        using var p = new Parallel(workers: 2, lateness: 10);

        // Tick 0: worker 0 sees 100, worker 1 sees 10. The global max is 100 → frontier 90 on both.
        p.Input.Push(Delta([new Event(100, 0), new Event(10, 1)]));
        p.Circuit.Step();
        Assert.All(p.Frontiers, f => Assert.Equal(90, f.Value));

        // Tick 1: 50 lands on worker 1, whose own max (10) would admit it; the serial path drops it.
        p.Input.Push(Delta([new Event(50, 1), new Event(95, 3)]));
        p.Circuit.Step();
        var output = p.Output.Current;
        Assert.Equal(Z64.Zero, output.WeightOf(new Event(50, 1)));
        Assert.Equal(Z64.One, output.WeightOf(new Event(95, 3)));
    }

    [Fact]
    public void ThresholdIsTheStartOfTickFrontier_NotThisTicksMax()
    {
        using var p = new Parallel(workers: 2, lateness: 10);
        p.Input.Push(Delta([new Event(100, 0)]));
        p.Circuit.Step();

        // In one tick, worker 0 raises the max to 200 while worker 1 gets 95: 95 is measured against
        // the frontier from before this tick (90), so it is admitted — as on the serial path.
        p.Input.Push(Delta([new Event(200, 0), new Event(95, 1)]));
        p.Circuit.Step();
        Assert.Equal(Z64.One, p.Output.Current.WeightOf(new Event(95, 1)));
        Assert.All(p.Frontiers, f => Assert.Equal(190, f.Value));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void MatchesSerial_TickByTick(int workers)
    {
        const long Lateness = 7;
        var serialFrontier = new MutableFrontier();
        InputHandle<ZSet<Event, Z64>>? serialIn = null;
        OutputHandle<ZSet<Event, Z64>>? serialOut = null;
        var serial = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Event, Z64>();
            serialIn = h;
            serialOut = b.Output(b.EnforceLateness(s, e => e.Time, Lateness, serialFrontier));
        });

        using var p = new Parallel(workers, Lateness);
        var random = new Random(1234 + workers);
        long id = 0;
        long clock = 0;
        for (var tick = 0; tick < 60; tick++)
        {
            clock += random.Next(0, 6);
            var rows = Enumerable.Range(0, random.Next(0, 8))
                .Select(_ => new Event(clock - random.Next(0, 15), id++))
                .ToList();
            serialIn!.Push(Delta(rows));
            serial.Step();
            p.Input.Push(Delta(rows));
            p.Circuit.Step();

            Assert.Equal(serialOut!.Current, p.Output.Current);
            Assert.All(p.Frontiers, f => Assert.Equal(serialFrontier.Value, f.Value));
        }
    }
    // Regression: a closure that captured the CircuitBuilder reaches the build context, and through
    // the global-max reduction every other replica's LatenessOperator. The stream-release scan must
    // not follow that, or replica 0 plans releases of replica 1's streams and races its thread.
    [Fact]
    public void StreamReleaseScan_StaysInsideItsOwnReplica()
    {
        using var pc = ParallelCircuit.Build(2, b =>
        {
            var (_, s) = b.ZSetInput<Event, Z64>("in");
            var enforced = b.EnforceLateness(s, e => e.Time, 5, new MutableFrontier());
            b.Output(b.Filter(enforced, e => b is not null && e.Id >= 0), "out");
        });

        var mine = StreamRelease.ScanReaders(pc.Replicas[0].Operators).ByStream.Keys;
        var theirs = StreamRelease.ScanReaders(pc.Replicas[1].Operators).ByStream.Keys
            .ToHashSet(ReferenceEqualityComparer.Instance);
        Assert.DoesNotContain(mine, s => theirs.Contains(s));
    }
}
