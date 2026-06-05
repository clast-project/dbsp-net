// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Tests.Circuit;

public class ParallelCircuitTests
{
    [Fact]
    public void Build_RejectsNonPositiveWorkers()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ParallelCircuit.Build(0, _ => { }));
        Assert.Throws<ArgumentOutOfRangeException>(() => ParallelCircuit.Build(-1, _ => { }));
    }

    [Fact]
    public void Workers_ReportsReplicaCount()
    {
        using var pc = ParallelCircuit.Build(4, _ => { });
        Assert.Equal(4, pc.Workers);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    public void Replicas_RunIndependently_AndGatherEqualsSingleCircuit(int workers)
    {
        // Each worker doubles its own input shard. Phase 1 has no sharded I/O,
        // so the host partitions manually: worker w gets the rows whose key
        // hashes to w. Gathering the per-worker outputs via Z-set Plus must
        // reconstruct exactly the single-circuit result over the whole input.
        static Stream<ZSet<int, Z64>> Doubled(CircuitBuilder b, out InputHandle<ZSet<int, Z64>> ih)
        {
            var (h, s) = b.Input<ZSet<int, Z64>>(ZSet.Empty<int, Z64>(), (x, y) => x.Plus(y), name: "in");
            ih = h;
            return b.Apply(s, z => z.ScalarMultiply(new Z64(2)));
        }

        // Reference: one circuit over all rows.
        var rows = Enumerable.Range(0, 50).ToArray();
        InputHandle<ZSet<int, Z64>>? refIn = null;
        OutputHandle<ZSet<int, Z64>>? refOut = null;
        var reference = RootCircuit.Build(b =>
        {
            refOut = b.Output(Doubled(b, out var h));
            refIn = h;
        });
        refIn!.Push(ZSet.FromKeys<int, Z64>(rows));
        reference.Step();
        var expected = refOut!.Current;

        // Parallel: partition rows across workers, gather outputs.
        using var pc = ParallelCircuit.Build(workers, b => b.Output(Doubled(b, out _), name: "out"));
        for (var w = 0; w < workers; w++)
        {
            var shard = rows.Where(r => (int)((uint)r % workers) == w);
            pc.WorkerInput<ZSet<int, Z64>>("in", w).Push(ZSet.FromKeys<int, Z64>(shard));
        }

        pc.Step();

        var gathered = ZSet.Empty<int, Z64>();
        for (var w = 0; w < workers; w++)
        {
            gathered = gathered.Plus(pc.WorkerOutput<ZSet<int, Z64>>("out", w).Current);
        }

        Assert.Equal(expected, gathered);
    }

    [Fact]
    public void Step_AdvancesTickCount_AndEveryReplicaActuallyStepped()
    {
        const int workers = 3;
        using var pc = ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<int>(0, (x, y) => x + y, name: "in");
            b.Output(b.Apply(s, v => v + 1), name: "out");
        });

        Assert.Equal(0, pc.TickCount);

        // Distinct value per worker; after one Step each worker's own output
        // must reflect its own input running through its own replica's Apply.
        for (var w = 0; w < workers; w++)
        {
            pc.WorkerInput<int>("in", w).Push(w * 10);
        }

        pc.Step();
        Assert.Equal(1, pc.TickCount);
        for (var w = 0; w < workers; w++)
        {
            Assert.Equal((w * 10) + 1, pc.WorkerOutput<int>("out", w).Current);
        }

        pc.Step();
        Assert.Equal(2, pc.TickCount);
    }

    [Fact]
    public void AdvanceTime_SetsTheClock_AndEnforcesMonotonicity()
    {
        using var pc = ParallelCircuit.Build(4, _ => { });

        pc.AdvanceTime(12_345);
        Assert.Equal(12_345, pc.LogicalTime);

        pc.AdvanceTime(20_000);
        Assert.Equal(20_000, pc.LogicalTime);

        // The per-replica monotonicity guard (RootCircuit.AdvanceTime) fires
        // through the fan-out: a backward move is rejected and the clock holds.
        Assert.Throws<ArgumentOutOfRangeException>(() => pc.AdvanceTime(19_999));
        Assert.Equal(20_000, pc.LogicalTime);
    }

    [Fact]
    public async Task Step_RunsWorkersConcurrently()
    {
        // If the W workers ran serially this barrier would never trip and the
        // step would hang; the timeout turns that into a failing test.
        const int workers = 4;
        using var rendezvous = new Barrier(workers);
        var observed = 0;
        using var pc = ParallelCircuit.Build(workers, b =>
        {
            var (_, s) = b.Input<int>(0, (x, y) => x + y, name: "in");
            b.Output(b.Apply(s, v =>
            {
                // Every worker must be inside Apply simultaneously to pass the
                // barrier — proves genuine parallelism, not interleaving.
                rendezvous.SignalAndWait(TimeSpan.FromSeconds(5));
                Interlocked.Increment(ref observed);
                return v;
            }), name: "out");
        });

        await Task.Run(pc.Step).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(workers, Volatile.Read(ref observed));
    }

    [Fact]
    public async Task Step_SurfacesWorkerExceptionsWithoutHanging()
    {
        using var pc = ParallelCircuit.Build(3, b =>
        {
            var (_, s) = b.Input<int>(0, (x, y) => x + y, name: "in");
            b.Output(b.Apply(s, v => v == 0 ? throw new InvalidOperationException("boom") : v), name: "out");
        });

        // Worker 1 pushes a non-zero value (no throw); workers 0 and 2 step on
        // the zero default and throw. The driver must collect both and return —
        // the timeout guards against a hang if a thrower stranded the barrier.
        pc.WorkerInput<int>("in", 1).Push(5);

        var agg = await Assert.ThrowsAsync<AggregateException>(
            () => Task.Run(pc.Step).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(2, agg.InnerExceptions.Count);
        Assert.All(agg.InnerExceptions, e => Assert.IsType<InvalidOperationException>(e));
    }

    [Fact]
    public void SingleWorker_StepsInlineAndProducesResult()
    {
        using var pc = ParallelCircuit.Build(1, b =>
        {
            var (_, s) = b.Input<int>(0, (x, y) => x + y, name: "in");
            b.Output(b.Apply(s, v => v * 3), name: "out");
        });

        pc.WorkerInput<int>("in", 0).Push(7);
        pc.Step();
        Assert.Equal(21, pc.WorkerOutput<int>("out", 0).Current);
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var pc = ParallelCircuit.Build(4, _ => { });
        pc.Step();
        pc.Dispose();
        pc.Dispose(); // no throw
        Assert.Throws<ObjectDisposedException>(() => pc.Step());
    }

    [Fact]
    public void WorkerInput_RejectsUnknownNameWorkerAndType()
    {
        using var pc = ParallelCircuit.Build(2, b =>
        {
            var (_, s) = b.Input<int>(0, (x, y) => x + y, name: "in");
            b.Output(s, name: "out");
        });

        Assert.Throws<KeyNotFoundException>(() => pc.WorkerInput<int>("missing", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => pc.WorkerInput<int>("in", 2));
        Assert.Throws<ArgumentOutOfRangeException>(() => pc.WorkerInput<int>("in", -1));
        // "out" is an OutputHandle; asking for it as an input is a type error.
        Assert.Throws<InvalidOperationException>(() => pc.WorkerInput<int>("out", 0));
    }

    [Fact]
    public void DuplicateNamedPort_IsABuildError()
    {
        Assert.Throws<ArgumentException>(() => ParallelCircuit.Build(2, b =>
        {
            b.Input<int>(0, (x, y) => x + y, name: "dup");
            b.Input<int>(0, (x, y) => x + y, name: "dup");
        }));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    public void RunDataParallel_RunsEveryWorkerOnce(int workers)
    {
        using var pc = ParallelCircuit.Build(workers, _ => { });

        var ran = new int[workers];
        pc.RunDataParallel((worker, _) => Interlocked.Increment(ref ran[worker]));

        Assert.All(ran, count => Assert.Equal(1, count));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    public void RunDataParallel_PhaseSync_SeparatesPhases(int workers)
    {
        // Phase 1: each worker publishes a value. Sync. Phase 2: each worker reads
        // the whole array and sums it. The barrier guarantees every phase-1 write
        // is visible before any phase-2 read, so every worker must see the full sum.
        using var pc = ParallelCircuit.Build(workers, _ => { });

        var published = new int[workers];
        var sums = new int[workers];
        var expected = workers * (workers + 1) / 2;   // sum of 1..workers

        pc.RunDataParallel((worker, sync) =>
        {
            published[worker] = worker + 1;
            sync.Sync();
            var total = 0;
            for (var i = 0; i < workers; i++)
            {
                total += published[i];
            }

            sums[worker] = total;
        });

        Assert.All(sums, s => Assert.Equal(expected, s));
    }

    [Fact]
    public async Task RunDataParallel_FaultBeforeSync_SurfacesWithoutHanging()
    {
        // Worker 0 throws in phase 1, before the rendezvous; the others are parked
        // at Sync(). The abort must release them (else the worker barrier deadlocks),
        // and the controller must surface the root cause. The timeout guards a hang.
        using var pc = ParallelCircuit.Build(3, _ => { });

        var agg = await Assert.ThrowsAsync<AggregateException>(() => Task.Run(() =>
            pc.RunDataParallel((worker, sync) =>
            {
                if (worker == 0)
                {
                    throw new InvalidOperationException("boom");
                }

                sync.Sync();
            })).WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.Single(agg.InnerExceptions);
        Assert.IsType<InvalidOperationException>(agg.InnerExceptions[0]);

        // A faulted circuit can no longer be driven.
        Assert.Throws<InvalidOperationException>(() => pc.Step());
    }

    [Fact]
    public void RunDataParallel_SingleWorker_RunsInlineWithNoopSync()
    {
        using var pc = ParallelCircuit.Build(1, _ => { });

        var ran = false;
        pc.RunDataParallel((worker, sync) =>
        {
            Assert.Equal(0, worker);
            sync.Sync();   // no-op for W == 1, must not block
            ran = true;
        });

        Assert.True(ran);
    }
}
