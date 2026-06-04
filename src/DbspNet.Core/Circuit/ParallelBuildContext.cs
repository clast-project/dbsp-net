// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Per-replica context threaded through a <see cref="ParallelCircuit"/> build so
/// the (otherwise identical) constructor closure can wire each replica's
/// exchange operators to the right worker identity and the right shared
/// coordinator. A plain <see cref="RootCircuit.Build(Action{CircuitBuilder})"/>
/// passes none, in which case <c>exchange</c> degrades to the identity.
/// </summary>
/// <remarks>
/// Every replica gets its own context (distinct <see cref="WorkerId"/>) but they
/// all share one <c>coordinators</c> registry. Replicas are built sequentially,
/// so the first replica's <see cref="NextCoordinator"/> calls create the
/// coordinators and later replicas reuse them by position: the i-th exchange in
/// every replica's identical operator sequence maps to <c>coordinators[i]</c>.
/// </remarks>
internal sealed class ParallelBuildContext
{
    private readonly List<IExchangeCoordinator> _coordinators;
    private int _nextExchange;

    internal ParallelBuildContext(
        int workerId, int workers, List<IExchangeCoordinator> coordinators, CancellationToken abort)
    {
        WorkerId = workerId;
        Workers = workers;
        _coordinators = coordinators;
        Abort = abort;
    }

    /// <summary>This replica's worker index in <c>[0, Workers)</c>.</summary>
    internal int WorkerId { get; }

    /// <summary>Total replica count <c>W</c>.</summary>
    internal int Workers { get; }

    /// <summary>
    /// Cancelled when any worker faults mid-tick, to release peers parked on an
    /// exchange barrier. Shared by all coordinators of this circuit.
    /// </summary>
    internal CancellationToken Abort { get; }

    /// <summary>
    /// The coordinator for the exchange at the current position in the build.
    /// The first replica to reach this position creates it via
    /// <paramref name="factory"/>; subsequent replicas reuse it. Not
    /// thread-safe — replicas are built sequentially.
    /// </summary>
    internal ExchangeCoordinator<T> NextCoordinator<T>(Func<ExchangeCoordinator<T>> factory)
    {
        var index = _nextExchange++;
        if (index == _coordinators.Count)
        {
            _coordinators.Add(factory());
        }

        return (ExchangeCoordinator<T>)_coordinators[index];
    }
}
