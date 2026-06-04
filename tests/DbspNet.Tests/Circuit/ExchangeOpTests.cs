// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Circuit;

public class ExchangeOpTests
{
    // A one-exchange circuit: a named Z-set input, shuffled by `partition`,
    // exposed as a named output. Each worker is fed its own shard via "in".
    private static ParallelCircuit BuildShuffle(int workers, Func<int, int> partition) =>
        ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), (x, y) => x.Plus(y), name: "in");
            b.Output(b.Exchange(s, partition), name: "out");
        });

    private static ZSet<int, Z64> Out(ParallelCircuit pc, int worker) =>
        pc.WorkerOutput<ZSet<int, Z64>>("out", worker).Current;

    private static ZSet<int, Z64> Gather(ParallelCircuit pc)
    {
        var gathered = ZSet.Empty<int, Z64>();
        for (var w = 0; w < pc.Workers; w++)
        {
            gathered = gathered.Plus(Out(pc, w));
        }

        return gathered;
    }

    private static int Bucket(int hash, int workers) => ((hash % workers) + workers) % workers;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void Exchange_ConservesTheInputMultiset_RegardlessOfW(int workers)
    {
        using var pc = BuildShuffle(workers, k => k);

        // A lopsided distribution — everything pushed to worker 0 — so the
        // shuffle has real work to do for W > 1. The redistribution must
        // preserve the multiset: gathering the outputs reconstructs the input.
        var all = ZSet.FromEntries(new[]
        {
            (1, new Z64(2)), (2, new Z64(-1)), (5, new Z64(3)),
            (9, new Z64(1)), (13, new Z64(7)), (4, new Z64(-2)), (8, new Z64(5)),
        });
        pc.WorkerInput<ZSet<int, Z64>>("in", 0).Push(all);

        pc.Step();

        Assert.Equal(all, Gather(pc));
    }

    [Fact]
    public void Exchange_CoLocatesEachKeyOnItsHashWorker()
    {
        const int workers = 4;
        using var pc = BuildShuffle(workers, k => k);

        // Spread the input across workers; after the exchange each worker must
        // hold only the keys whose hash class is its own.
        for (var w = 0; w < workers; w++)
        {
            pc.WorkerInput<ZSet<int, Z64>>("in", w).Push(ZSet.FromKeys<int, Z64>(Enumerable.Range(w * 7, 7)));
        }

        pc.Step();

        for (var w = 0; w < workers; w++)
        {
            foreach (var (key, _) in Out(pc, w))
            {
                Assert.Equal(w, Bucket(key, workers));
            }
        }
    }

    [Fact]
    public void Exchange_WithSingleWorker_IsIdentityPassthrough()
    {
        // W == 1 adds no exchange operator; the stream passes through unchanged.
        using var pc = BuildShuffle(1, k => k);
        var all = ZSet.FromKeys<int, Z64>(Enumerable.Range(0, 20));
        pc.WorkerInput<ZSet<int, Z64>>("in", 0).Push(all);

        pc.Step();

        Assert.Equal(all, Out(pc, 0));
    }

    [Fact]
    public void Exchange_HandlesNegativePartitionHashes()
    {
        const int workers = 4;
        using var pc = BuildShuffle(workers, k => -k); // negative for positive keys

        var all = ZSet.FromKeys<int, Z64>(Enumerable.Range(1, 30));
        pc.WorkerInput<ZSet<int, Z64>>("in", 0).Push(all);

        pc.Step();

        Assert.Equal(all, Gather(pc));
        for (var w = 0; w < workers; w++)
        {
            foreach (var (key, _) in Out(pc, w))
            {
                Assert.Equal(w, Bucket(-key, workers));
            }
        }
    }

    [Fact]
    public void TwoExchanges_InOneCircuit_UseDistinctCoordinators_AndConserve()
    {
        const int workers = 4;
        using var pc = ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), (x, y) => x.Plus(y), name: "in");
            var first = b.Exchange(s, k => k);
            var shifted = b.Apply(first, z => z.MapKeys(k => k + 1));
            b.Output(b.Exchange(shifted, k => k), name: "out");
        });

        var all = ZSet.FromKeys<int, Z64>(Enumerable.Range(0, 40));
        pc.WorkerInput<ZSet<int, Z64>>("in", 0).Push(all);

        pc.Step();

        // If the two exchanges shared a coordinator the second's barrier would
        // collide with the first's; correctness here implies they are distinct.
        var expected = all.MapKeys(k => k + 1);
        Assert.Equal(expected, Gather(pc));
        for (var w = 0; w < workers; w++)
        {
            foreach (var (key, _) in Out(pc, w))
            {
                Assert.Equal(w, Bucket(key, workers));
            }
        }
    }

    [Fact]
    public void Exchange_IsDeterministic_AcrossInstances()
    {
        static ZSet<int, Z64>[] RunOnce()
        {
            const int workers = 4;
            using var pc = BuildShuffle(workers, k => k);
            for (var w = 0; w < workers; w++)
            {
                pc.WorkerInput<ZSet<int, Z64>>("in", w).Push(ZSet.FromKeys<int, Z64>(Enumerable.Range(w * 11, 13)));
            }

            pc.Step();
            return [Out(pc, 0), Out(pc, 1), Out(pc, 2), Out(pc, 3)];
        }

        var a = RunOnce();
        var b = RunOnce();
        for (var w = 0; w < a.Length; w++)
        {
            Assert.Equal(a[w], b[w]);
        }
    }

    [Fact]
    public void Exchange_Stress_ConservesAndCoLocates_OverManyTicks()
    {
        const int workers = 8;
        const int ticks = 200;
        using var pc = BuildShuffle(workers, k => k);
        var rng = new Random(0xC0FFEE);

        for (var tick = 0; tick < ticks; tick++)
        {
            var union = ZSet.Empty<int, Z64>();
            for (var w = 0; w < workers; w++)
            {
                var entries = new List<(int, Z64)>();
                var n = rng.Next(0, 12);
                for (var i = 0; i < n; i++)
                {
                    entries.Add((rng.Next(0, 64), new Z64(rng.Next(1, 4))));
                }

                var shard = ZSet.FromEntries(entries);
                pc.WorkerInput<ZSet<int, Z64>>("in", w).Push(shard);
                union = union.Plus(shard);
            }

            pc.Step();

            var gathered = ZSet.Empty<int, Z64>();
            for (var w = 0; w < workers; w++)
            {
                var outW = Out(pc, w);
                foreach (var (key, _) in outW)
                {
                    Assert.Equal(w, Bucket(key, workers));
                }

                gathered = gathered.Plus(outW);
            }

            Assert.Equal(union, gathered);
        }
    }

    [Fact]
    public async Task Exchange_MidTickPartitionThrow_SurfacesWithoutHanging_AndPoisons()
    {
        const int workers = 4;
        using var pc = ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), (x, y) => x.Plus(y), name: "in");
            b.Output(
                b.Exchange(s, k => k == 999 ? throw new InvalidOperationException("bad key") : k),
                name: "out");
        });

        // Worker 0 carries the poison key and bails out of the tick before
        // publishing; workers 1..3 publish and park on the exchange barrier.
        // The abort must release them so the controller never hangs.
        pc.WorkerInput<ZSet<int, Z64>>("in", 0).Push(ZSet.FromKeys<int, Z64>([999, 1, 2]));
        for (var w = 1; w < workers; w++)
        {
            pc.WorkerInput<ZSet<int, Z64>>("in", w).Push(ZSet.FromKeys<int, Z64>([w, w + 10]));
        }

        var agg = await Assert.ThrowsAsync<AggregateException>(
            () => Task.Run(pc.Step).WaitAsync(TimeSpan.FromSeconds(10)));

        // The root cause surfaces; the peers' cascaded cancellations are filtered.
        Assert.Contains(agg.InnerExceptions, e => e is InvalidOperationException);
        Assert.DoesNotContain(agg.InnerExceptions, e => e is OperationCanceledException);

        // A mid-tick fault poisons the run: further steps are refused.
        Assert.Throws<InvalidOperationException>(pc.Step);
    }
}
