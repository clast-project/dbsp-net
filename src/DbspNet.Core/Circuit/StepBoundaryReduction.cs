// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// A combine across replicas that a <see cref="ParallelCircuit"/> runs on the driver thread after
/// every <see cref="ParallelCircuit.Step"/>, between the "done" rendezvous and the next "go": every
/// worker is parked, so the reduction reads and writes replica state with no extra barrier and no
/// exchange (docs/design-parallel-frontier.md §2 Q2). The barriers on either side are full fences,
/// so the workers observe the result when they next wake.
/// </summary>
internal interface IStepBoundaryReduction
{
    /// <summary>Combine the participants' values and hand the result back to each. Driver thread
    /// only, with every worker parked.</summary>
    void Reduce();
}

/// <summary>One replica's side of a <see cref="GlobalMaxReduction"/>.</summary>
internal interface IGlobalMaxParticipant
{
    /// <summary>This replica's local maximum, if it has seen any value.</summary>
    bool TryGetLocalMax(out long max);

    /// <summary>Adopt the maximum over all replicas (never lower than this replica's own).</summary>
    void AdoptGlobalMax(long max);
}

/// <summary>
/// The scalar all-reduce behind a parallel <c>LATENESS</c> frontier: the maximum over the W
/// same-position participants (one per replica), adopted by all of them. Feldera does the same as a
/// one-scalar exchange per step (<c>dyn_waterline_monotonic</c>); here the driver folds it between
/// steps, so it costs W reads and W writes, not a rendezvous.
/// </summary>
internal sealed class GlobalMaxReduction : IStepBoundaryReduction
{
    private readonly IGlobalMaxParticipant?[] _participants;

    internal GlobalMaxReduction(int workers) => _participants = new IGlobalMaxParticipant?[workers];

    internal void Join(int worker, IGlobalMaxParticipant participant)
    {
        if (_participants[worker] is not null)
        {
            throw new InvalidOperationException(
                $"worker {worker} joined the same global-max reduction twice; replicas must build identical graphs");
        }

        _participants[worker] = participant;
    }

    public void Reduce()
    {
        var any = false;
        var max = long.MinValue;
        foreach (var p in _participants)
        {
            if (p!.TryGetLocalMax(out var m) && (!any || m > max))
            {
                max = m;
                any = true;
            }
        }

        if (!any)
        {
            return;
        }

        foreach (var p in _participants)
        {
            p!.AdoptGlobalMax(max);
        }
    }
}
