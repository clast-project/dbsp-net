// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit;

/// <summary>
/// A data-parallel SPMD driver: <c>W</c> identical replicas of one circuit, each
/// owning a dedicated thread, advanced together one tick at a time.
/// </summary>
/// <remarks>
/// <para>
/// The model is Feldera's: instantiate the whole circuit <c>W</c> times — once
/// per worker — by running the same constructor closure through
/// <see cref="RootCircuit.Build"/>, then process a disjoint partition of the
/// data on each replica. Because the closure is pure and operator state is
/// isolated per circuit, the <c>W</c> replicas share nothing during a tick;
/// replication is "free" and the operators are reused unchanged. Merging the
/// replicas' outputs (Z-set union) reconstructs the single-worker result, so
/// correctness is independent of <c>W</c>.
/// </para>
/// <para>
/// This is Phase 1 — the driver only. It coordinates <c>W</c> replicas through
/// a per-tick barrier; it does <em>not</em> yet split inputs or gather outputs
/// (sharded I/O) or shuffle between replicas (the <c>exchange</c> operator).
/// Until those land, each replica runs fully independently over whatever data
/// the host pushes to <em>its</em> copy of an input — reach a worker's copy of
/// a named port via <see cref="WorkerInput{T}"/> / <see cref="WorkerOutput{T}"/>.
/// </para>
/// <para>
/// Threading: each replica runs on its own long-lived background thread parked
/// on a shared barrier. <see cref="Step"/> releases all workers, then blocks
/// until every one has finished its tick. As a special case <c>W == 1</c> spawns
/// no thread and steps inline on the caller — the single-threaded hot path is
/// preserved byte-for-byte. The driver itself never reads the wall clock, so a
/// run is as deterministic as the underlying circuit.
/// </para>
/// </remarks>
public sealed class ParallelCircuit : IDisposable
{
    private readonly RootCircuit[] _replicas;
    private readonly IReadOnlyList<IExchangeCoordinator> _coordinators;
    private readonly CancellationTokenSource _abort;
    private readonly Thread[]? _threads;
    private readonly Barrier? _barrier;
    private readonly Barrier? _workerBarrier;
    private readonly IPhaseSync? _phaseSync;
    private readonly Action<int> _stepJob;
    private readonly Exception?[] _stepErrors;
    private volatile Action<int>? _job;
    private volatile bool _stopping;
    private bool _faulted;
    private bool _disposed;

    private ParallelCircuit(
        RootCircuit[] replicas, IReadOnlyList<IExchangeCoordinator> coordinators, CancellationTokenSource abort)
    {
        _replicas = replicas;
        _coordinators = coordinators;
        _abort = abort;
        _stepErrors = new Exception?[replicas.Length];
        _stepJob = StepJob;

        if (replicas.Length == 1)
        {
            // Single worker: no thread, no barrier — Step runs inline so the
            // hot path is identical to a plain RootCircuit.
            _threads = null;
            _barrier = null;
            return;
        }

        // W + 1 participants: the W workers plus the controlling thread that
        // calls Step. Each tick is two rendezvous — "go" then "done" — so the
        // controller and workers stay in lockstep with no scheduler.
        _barrier = new Barrier(replicas.Length + 1);

        // A workers-only barrier (the controller is not a participant) lets a
        // RunDataParallel job rendezvous its W workers between phases — e.g. the
        // encode→scatter boundary of a parallel ingest. It is abort-aware (the
        // same CancellationToken the exchange uses) so a worker that throws in an
        // early phase releases its peers instead of stranding them.
        _workerBarrier = new Barrier(replicas.Length);
        _phaseSync = new WorkerPhaseSync(_workerBarrier, _abort.Token);
        _threads = new Thread[replicas.Length];
        for (var w = 0; w < replicas.Length; w++)
        {
            var worker = w;
            var thread = new Thread(() => WorkerLoop(worker))
            {
                IsBackground = true,
                Name = $"DbspWorker-{worker}",
            };
            _threads[w] = thread;
            thread.Start();
        }
    }

    /// <summary>
    /// Build <paramref name="workers"/> identical replicas of a circuit by
    /// running <paramref name="configure"/> once per worker. The replicas are
    /// constructed sequentially on the calling thread (building is pure); the
    /// worker threads are spawned and parked, ready for the first
    /// <see cref="Step"/>.
    /// </summary>
    /// <param name="workers">Number of replicas; must be at least 1.</param>
    /// <param name="configure">
    /// The same constructor closure passed to <see cref="RootCircuit.Build"/>.
    /// It is run <paramref name="workers"/> times and must build the identical
    /// graph each time — give it no per-worker behavior. Name the ports you
    /// need to drive (see <see cref="CircuitBuilder.Input{T}"/>).
    /// </param>
    public static ParallelCircuit Build(int workers, Action<CircuitBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentOutOfRangeException.ThrowIfLessThan(workers, 1);

        // One coordinator registry and one abort signal shared across replicas;
        // the per-replica context carries the worker id. Replicas are built
        // sequentially so the registry fills without locking.
        var coordinators = new List<IExchangeCoordinator>();
        var abort = new CancellationTokenSource();
        var replicas = new RootCircuit[workers];
        for (var w = 0; w < workers; w++)
        {
            var context = new ParallelBuildContext(w, workers, coordinators, abort.Token);
            replicas[w] = RootCircuit.Build(configure, context);
        }

        return new ParallelCircuit(replicas, coordinators, abort);
    }

    /// <summary>Number of replicas (the <c>W</c> the driver coordinates).</summary>
    public int Workers => _replicas.Length;

    /// <summary>
    /// The <c>W</c> replica circuits, in worker order. Exposed to the persistence
    /// layer so it can snapshot/restore each replica's operator state into a
    /// per-worker subtree. Only safe to read between <see cref="Step"/>s, when the
    /// worker threads are parked on the barrier and no replica is mutating.
    /// </summary>
    internal IReadOnlyList<RootCircuit> Replicas
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_faulted)
            {
                throw new InvalidOperationException(
                    "The parallel circuit faulted on a previous step; its replicas have diverged " +
                    "and cannot be snapshotted.");
            }

            return _replicas;
        }
    }

    /// <summary>
    /// Number of completed ticks. Every replica advances identically, so this
    /// reads replica 0.
    /// </summary>
    public long TickCount => _replicas[0].TickCount;

    /// <summary>
    /// The current logical clock (microseconds since the Unix epoch). All
    /// replicas hold the same value — set via <see cref="AdvanceTime"/> — so
    /// this reads replica 0. See <see cref="RootCircuit.LogicalTime"/>.
    /// </summary>
    public long LogicalTime => _replicas[0].LogicalTime;

    /// <summary>
    /// Advance every replica's logical clock to <paramref name="microsSinceEpoch"/>
    /// before a <see cref="Step"/>. Fans the value out to all workers (which are
    /// parked, so this is race-free). See <see cref="RootCircuit.AdvanceTime"/>.
    /// </summary>
    public void AdvanceTime(long microsSinceEpoch)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        foreach (var replica in _replicas)
        {
            replica.AdvanceTime(microsSinceEpoch);
        }
    }

    /// <summary>
    /// Advance every replica by one logical tick, running their <see cref="RootCircuit.Step"/>
    /// loops concurrently. Returns once all workers have finished the tick. If
    /// any worker's step throws, the others still complete and the failures are
    /// rethrown together as an <see cref="AggregateException"/>.
    /// </summary>
    public void Step()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
        {
            throw new InvalidOperationException(
                "The parallel circuit faulted on a previous step and can no longer be stepped.");
        }

        if (_barrier is null)
        {
            // W == 1: run inline, no handoff. A throw propagates directly — the
            // single replica's state is no more inconsistent than a plain
            // RootCircuit's would be, so there is nothing extra to poison.
            _replicas[0].Step();
            return;
        }

        Dispatch(_stepJob);

        // A mid-tick fault means the replicas have diverged (some operators ran,
        // some did not); the run is unrecoverable.
        ThrowOnFailures("One or more workers threw while stepping the parallel circuit.");
    }

    /// <summary>
    /// Run a data-parallel job across the workers: each worker <c>w</c> runs
    /// <paramref name="job"/><c>(w, sync)</c> on its own thread, with
    /// <paramref name="job"/> free to call <see cref="IPhaseSync.Sync"/> to
    /// rendezvous all workers between phases (every worker must call it the same
    /// number of times). Used for parallel ingest (encode then scatter) and
    /// egest (decode). Returns once all workers finish; faults are collected and
    /// rethrown as an <see cref="AggregateException"/> exactly like
    /// <see cref="Step"/>. As with <see cref="Step"/>, <c>W == 1</c> runs inline
    /// on the caller with a no-op <see cref="IPhaseSync"/>.
    /// </summary>
    /// <remarks>
    /// Intended to run between steps, when the worker threads are parked — drive
    /// it from the same thread that calls <see cref="Step"/>, not concurrently.
    /// </remarks>
    public void RunDataParallel(Action<int, IPhaseSync> job)
    {
        ArgumentNullException.ThrowIfNull(job);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_faulted)
        {
            throw new InvalidOperationException(
                "The parallel circuit faulted on a previous step and can no longer be stepped.");
        }

        if (_barrier is null)
        {
            // W == 1: inline, no handoff, no barrier — the single-threaded path
            // is preserved and Sync() is a no-op.
            job(0, NoopPhaseSync.Instance);
            return;
        }

        Dispatch(worker => job(worker, _phaseSync!));
        ThrowOnFailures("One or more workers threw while running a data-parallel job.");
    }

    /// <summary>
    /// Hand <paramref name="job"/> to the parked workers and block until every one
    /// has finished it: clear the error slots, set the job, then the two
    /// "go"/"done" rendezvous. Only valid when <see cref="_barrier"/> is non-null
    /// (W &gt; 1).
    /// </summary>
    private void Dispatch(Action<int> job)
    {
        _job = job;
        Array.Clear(_stepErrors);
        _barrier!.SignalAndWait();   // "go": release the parked workers
        _barrier.SignalAndWait();    // "done": rendezvous after every worker's job
        _job = null;
    }

    private void StepJob(int worker) => _replicas[worker].Step();

    /// <summary>Poison the run and rethrow if the last dispatch recorded any fault.</summary>
    private void ThrowOnFailures(string message)
    {
        var failures = CollectFailures();
        if (failures is not null)
        {
            _faulted = true;
            throw new AggregateException(message, failures);
        }
    }

    /// <summary>
    /// The worker exceptions from the last step, or <see langword="null"/> if
    /// none. Cascaded cancellations — peers released from an exchange barrier by
    /// the first fault's abort — are dropped in favor of the root cause(s); only
    /// if every recorded fault is a cancellation are those returned.
    /// </summary>
    private List<Exception>? CollectFailures()
    {
        List<Exception>? roots = null;
        List<Exception>? cancellations = null;
        foreach (var error in _stepErrors)
        {
            if (error is null)
            {
                continue;
            }

            if (error is OperationCanceledException)
            {
                (cancellations ??= []).Add(error);
            }
            else
            {
                (roots ??= []).Add(error);
            }
        }

        return roots ?? cancellations;
    }

    /// <summary>
    /// The <paramref name="worker"/>-th replica's copy of the input registered
    /// under <paramref name="name"/> (see <see cref="CircuitBuilder.Input{T}"/>).
    /// Phase 1 exposes per-worker handles directly; sharded input that splits a
    /// single push across workers is a later phase.
    /// </summary>
    public InputHandle<T> WorkerInput<T>(string name, int worker) =>
        Port<InputHandle<T>>(name, worker);

    /// <summary>
    /// The <paramref name="worker"/>-th replica's copy of the output registered
    /// under <paramref name="name"/> (see <see cref="CircuitBuilder.Output{T}"/>).
    /// Gather across all workers — e.g. Z-set <c>Plus</c> — to reconstruct the
    /// single-circuit result.
    /// </summary>
    public OutputHandle<T> WorkerOutput<T>(string name, int worker) =>
        Port<OutputHandle<T>>(name, worker);

    /// <summary>
    /// A single sharded input over all workers' copies of the input named
    /// <paramref name="name"/>: <see cref="ShardedInputHandle{TKey,TWeight}.Push"/>
    /// splits a Z-set across the replicas by <paramref name="partition"/> (use
    /// <see cref="StableHash"/> for a recovery-stable hash). The host pushes one
    /// logical input instead of addressing each worker by hand.
    /// </summary>
    public ShardedInputHandle<TKey, TWeight> ShardedInput<TKey, TWeight>(string name, Func<TKey, int> partition)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        ArgumentNullException.ThrowIfNull(partition);
        var shards = new InputHandle<ZSet<TKey, TWeight>>[_replicas.Length];
        for (var w = 0; w < _replicas.Length; w++)
        {
            shards[w] = WorkerInput<ZSet<TKey, TWeight>>(name, w);
        }

        return new ShardedInputHandle<TKey, TWeight>(shards, partition);
    }

    /// <summary>
    /// A single sharded output over all workers' copies of the output named
    /// <paramref name="name"/>: <see cref="ShardedOutputHandle{TKey,TWeight}.Current"/>
    /// gathers the per-replica outputs (Z-set sum) into the single-circuit result.
    /// </summary>
    public ShardedOutputHandle<TKey, TWeight> ShardedOutput<TKey, TWeight>(string name)
        where TKey : notnull
        where TWeight : struct, IZRing<TWeight>
    {
        var shards = new OutputHandle<ZSet<TKey, TWeight>>[_replicas.Length];
        for (var w = 0; w < _replicas.Length; w++)
        {
            shards[w] = WorkerOutput<ZSet<TKey, TWeight>>(name, w);
        }

        return new ShardedOutputHandle<TKey, TWeight>(shards);
    }

    private TPort Port<TPort>(string name, int worker)
        where TPort : class
    {
        ArgumentNullException.ThrowIfNull(name);
        if ((uint)worker >= (uint)_replicas.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(worker), worker, $"worker must be in [0, {_replicas.Length}).");
        }

        var port = _replicas[worker].FindNamedPort(name)
            ?? throw new KeyNotFoundException(
                $"No circuit port named '{name}'. Pass a name to Input/Output when building.");

        return port as TPort
            ?? throw new InvalidOperationException(
                $"Circuit port '{name}' is a {port.GetType().Name}, not a {typeof(TPort).Name}.");
    }

    private void WorkerLoop(int worker)
    {
        var barrier = _barrier!;
        while (true)
        {
            barrier.SignalAndWait();   // "go"
            if (_stopping)
            {
                return;
            }

            try
            {
                _job!(worker);
            }
            catch (Exception ex)
            {
                // Record, then trip the abort so any peers parked on an exchange
                // (or this job's inter-phase) barrier are released — they would
                // otherwise wait for a worker that has bailed out. Keep going to
                // the "done" barrier — a throwing worker must not strand the
                // controller waiting forever.
                _stepErrors[worker] = ex;
                _abort.Cancel();
            }

            barrier.SignalAndWait();   // "done"
        }
    }

    /// <summary>
    /// Stop the worker threads and release resources. Idempotent. Do not call
    /// <see cref="Step"/> concurrently with <see cref="Dispose"/>.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_barrier is not null)
        {
            _stopping = true;
            _barrier.SignalAndWait();   // "go": workers wake, observe _stopping, return
            foreach (var thread in _threads!)
            {
                thread.Join();
            }

            _barrier.Dispose();
            _workerBarrier!.Dispose();
        }

        // Threads are joined (or never started, for W == 1), so no worker can be
        // inside an exchange; safe to release the coordinators and abort signal.
        foreach (var coordinator in _coordinators)
        {
            coordinator.Dispose();
        }

        _abort.Dispose();
    }
}

/// <summary>
/// A mid-job rendezvous handed to a <see cref="ParallelCircuit.RunDataParallel"/>
/// job: calling <see cref="Sync"/> blocks until every worker has reached the same
/// point, separating one data-parallel phase from the next (e.g. an ingest's
/// parallel encode from its scatter). Every worker must call it the same number
/// of times. A no-op when <c>W == 1</c>, where the job runs inline.
/// </summary>
public interface IPhaseSync
{
    /// <summary>Block until all workers reach this point.</summary>
    void Sync();
}

/// <summary>The <c>W == 1</c> phase sync: nothing to wait for.</summary>
internal sealed class NoopPhaseSync : IPhaseSync
{
    internal static readonly NoopPhaseSync Instance = new();

    private NoopPhaseSync()
    {
    }

    public void Sync()
    {
    }
}

/// <summary>
/// The <c>W &gt; 1</c> phase sync: a workers-only <see cref="Barrier"/> waited on
/// with the run's abort token, so a worker that faults before the rendezvous
/// releases its peers (which then throw <see cref="OperationCanceledException"/>)
/// rather than deadlocking them.
/// </summary>
internal sealed class WorkerPhaseSync : IPhaseSync
{
    private readonly Barrier _barrier;
    private readonly CancellationToken _abort;

    internal WorkerPhaseSync(Barrier barrier, CancellationToken abort)
    {
        _barrier = barrier;
        _abort = abort;
    }

    public void Sync() => _barrier.SignalAndWait(_abort);
}
