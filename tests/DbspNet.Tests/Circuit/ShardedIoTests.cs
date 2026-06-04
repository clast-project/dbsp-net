// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Circuit;

public class ShardedIoTests
{
    private static readonly Func<ZSet<int, Z64>, ZSet<int, Z64>, ZSet<int, Z64>> Plus = (x, y) => x.Plus(y);

    // input --(map *2)--> output, no exchange: each replica transforms its own shard.
    private static ParallelCircuit BuildDoubler(int workers) =>
        ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), Plus, name: "in");
            b.Output(b.Apply(s, z => z.ScalarMultiply(new Z64(2))), name: "out");
        });

    // input --(exchange by key)--> output: re-shards before gathering.
    private static ParallelCircuit BuildExchange(int workers) =>
        ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), Plus, name: "in");
            b.Output(b.Exchange(s, StableHash.Of), name: "out");
        });

    private static int Bucket(int hash, int workers) => ((hash % workers) + workers) % workers;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void ShardedIo_RoundTrips_WithoutExchange(int workers)
    {
        using var pc = BuildDoubler(workers);
        var input = ZSet.FromEntries(new[]
        {
            (1, new Z64(2)), (2, new Z64(-1)), (5, new Z64(3)),
            (9, new Z64(1)), (13, new Z64(7)), (4, new Z64(-2)), (40, new Z64(5)),
        });

        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");

        inH.Push(input);
        pc.Step();

        Assert.Equal(input.ScalarMultiply(new Z64(2)), outH.Current);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void ShardedIo_RoundTrips_ThroughExchange(int workers)
    {
        // Input sharded by hash, then re-sharded by the exchange; the gather must
        // still reconstruct the input multiset (with cancelling weights).
        using var pc = BuildExchange(workers);
        var input = ZSet.FromEntries(new[]
        {
            (3, new Z64(1)), (3, new Z64(2)), (7, new Z64(-4)), (12, new Z64(9)),
            (100, new Z64(-1)), (101, new Z64(1)), (250, new Z64(6)),
        });

        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");

        inH.Push(input);
        pc.Step();

        Assert.Equal(input, outH.Current);
    }

    [Fact]
    public void ShardedInput_SplitsByStableHash()
    {
        const int workers = 4;
        using var pc = BuildDoubler(workers);
        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);

        inH.Push(ZSet.FromKeys<int, Z64>(Enumerable.Range(0, 40)));
        pc.Step();

        // Each worker's output may hold only keys whose stable-hash class is its own.
        for (var w = 0; w < workers; w++)
        {
            foreach (var (key, _) in pc.WorkerOutput<ZSet<int, Z64>>("out", w).Current)
            {
                Assert.Equal(w, Bucket(StableHash.Of(key), workers));
            }
        }
    }

    [Fact]
    public void ShardedInput_MergesMultiplePushesPerTick()
    {
        const int workers = 4;
        using var pc = BuildDoubler(workers);
        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");

        // Two pushes touching the same key must merge per shard (Plus), exactly
        // as a single circuit would merge them before the tick.
        inH.Push(ZSet.Singleton(7, new Z64(1)));
        inH.Push(ZSet.Singleton(7, new Z64(2)));
        inH.Push(ZSet.Singleton(3, new Z64(5)));
        pc.Step();

        var expected = ZSet.FromEntries(new[] { (7, new Z64(3)), (3, new Z64(5)) }).ScalarMultiply(new Z64(2));
        Assert.Equal(expected, outH.Current);
    }

    [Fact]
    public void ShardedInput_CancellingWeightsAcrossPushes_VanishFromOutput()
    {
        const int workers = 4;
        using var pc = BuildDoubler(workers);
        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");

        inH.Push(ZSet.Singleton(5, new Z64(3)));
        inH.Push(ZSet.Singleton(5, new Z64(-3))); // cancels on the same shard
        pc.Step();

        Assert.True(outH.Current.IsEmpty);
    }

    [Fact]
    public void ShardedIo_SingleWorker_GathersThroughTheOnlyReplica()
    {
        using var pc = BuildDoubler(1);
        var input = ZSet.FromKeys<int, Z64>(Enumerable.Range(0, 10));
        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");

        inH.Push(input);
        pc.Step();

        Assert.Equal(input.ScalarMultiply(new Z64(2)), outH.Current);
    }

    [Fact]
    public void ShardedIo_AccumulatesAcrossTicks_ThroughExchange()
    {
        const int workers = 8;
        using var pc = BuildExchange(workers);
        var inH = pc.ShardedInput<int, Z64>("in", StableHash.Of);
        var outH = pc.ShardedOutput<int, Z64>("out");
        var rng = new Random(0xBEEF);

        for (var tick = 0; tick < 50; tick++)
        {
            var entries = new List<(int, Z64)>();
            var n = rng.Next(0, 20);
            for (var i = 0; i < n; i++)
            {
                entries.Add((rng.Next(0, 100), new Z64(rng.Next(-3, 4))));
            }

            var delta = ZSet.FromEntries(entries);
            inH.Push(delta);
            pc.Step();

            // The exchange is stateless: each tick's gather is exactly that tick's
            // input redistributed, so it round-trips per tick.
            Assert.Equal(delta, outH.Current);
        }
    }
}
