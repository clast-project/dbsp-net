// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Non-generic handle for an exchange's shared state, so a
/// <see cref="ParallelBuildContext"/> can hold a registry of differently-typed
/// coordinators and dispose them uniformly.
/// </summary>
internal interface IExchangeCoordinator : IDisposable
{
}

/// <summary>
/// The cross-worker rendezvous for one <c>exchange</c> instance: a
/// <c>W×W</c> mailbox grid plus a <see cref="Barrier"/> over the <c>W</c>
/// worker threads. One coordinator is shared by every replica's copy of the
/// same exchange operator (the replicas reach it at the same point in their
/// identical operator sequences).
/// </summary>
/// <remarks>
/// Each tick, worker <c>w</c> writes its <em>row</em> (cells
/// <c>[w, 0..W-1]</c>) — the buckets it is sending to every destination — then
/// all workers rendezvous at <see cref="Wait"/>. After the barrier each worker
/// reads its <em>column</em> (<c>[0..W-1, w]</c>): the buckets every worker
/// addressed to it. Writers touch disjoint rows so there is no write/write
/// race; the barrier's release/acquire fence makes every row visible before
/// any column is read. The grid is fully overwritten each tick (a worker
/// publishes all <c>W</c> cells of its row, empty buckets included), and
/// <see cref="ParallelCircuit"/>'s end-of-tick barrier separates a tick's
/// reads from the next tick's writes — so no per-cell clearing is needed.
/// </remarks>
internal sealed class ExchangeCoordinator<T> : IExchangeCoordinator
{
    private readonly T[,] _mailbox;
    private readonly Barrier _barrier;

    internal ExchangeCoordinator(int workers)
    {
        Workers = workers;
        _mailbox = new T[workers, workers];
        _barrier = new Barrier(workers);
    }

    /// <summary>Number of workers <c>W</c> — the grid is <c>W×W</c>.</summary>
    internal int Workers { get; }

    /// <summary>Publish the bucket worker <paramref name="from"/> is sending to worker <paramref name="to"/>.</summary>
    internal void Publish(int from, int to, T payload) => _mailbox[from, to] = payload;

    /// <summary>
    /// Rendezvous: block until all <c>W</c> workers have published their rows.
    /// <paramref name="abort"/> releases waiters if another worker faults this
    /// tick (an exception is a conditional skip the barrier cannot otherwise
    /// survive); a released waiter throws <see cref="OperationCanceledException"/>.
    /// </summary>
    internal void Wait(CancellationToken abort) => _barrier.SignalAndWait(abort);

    /// <summary>Read the bucket worker <paramref name="from"/> sent to worker <paramref name="to"/>.</summary>
    internal T Read(int from, int to) => _mailbox[from, to];

    public void Dispose() => _barrier.Dispose();
}
