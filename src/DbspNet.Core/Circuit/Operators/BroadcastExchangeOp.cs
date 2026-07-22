// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// The all-gather (broadcast) exchange: replicates a sharded Z-set stream so
/// that, after it, <em>every</em> worker holds the union of all workers' shards.
/// One instance runs per replica; they coordinate through a shared
/// <see cref="ExchangeCoordinator{T}"/>.
/// </summary>
/// <remarks>
/// <para>Used to broadcast a small dimension for a broadcast join: instead of
/// hash-sharding both join sides by the (possibly low-cardinality, skewed) join
/// key — which lands a hot key's whole group on one worker — the large fact side
/// keeps its balanced partition and the small dimension is replicated to every
/// worker, so each worker joins its local fact shard against the complete
/// dimension. Summing the workers' outputs reconstructs the full join exactly,
/// because every fact row is on one worker and every worker sees the whole
/// dimension.</para>
/// <para>Self-synchronizing like <see cref="ExchangeOp{TKey,TWeight}"/>: each
/// worker publishes its whole shard to every cell of its grid row, rendezvous at
/// the barrier, then reads its column (every worker's shard) and unions. A given
/// input row lives on exactly one worker, so the union never double-counts.</para>
/// </remarks>
internal sealed class BroadcastExchangeOp<TRow, TWeight> : IOperator
    where TRow : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TRow, TWeight>> _input;
    private readonly Stream<ZSet<TRow, TWeight>> _output;
    private readonly ExchangeCoordinator<List<KeyValuePair<TRow, TWeight>>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal BroadcastExchangeOp(
        Stream<ZSet<TRow, TWeight>> input,
        Stream<ZSet<TRow, TWeight>> output,
        ExchangeCoordinator<List<KeyValuePair<TRow, TWeight>>> coordinator,
        int worker,
        CancellationToken abort)
    {
        _input = input;
        _output = output;
        _coordinator = coordinator;
        _worker = worker;
        _workers = coordinator.Workers;
        _abort = abort;
    }

    public void Step()
    {
        var workers = _workers;
        var profile = StepProfiler.Enabled;
        var t0 = profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0L;

        // This worker's whole shard becomes one bucket, published to every cell of
        // our grid row so any reader's column sees it. The Z-set's keys are already
        // distinct, so a plain list suffices.
        var shard = new List<KeyValuePair<TRow, TWeight>>();
        foreach (var kv in _input.Current)
        {
            shard.Add(kv);
        }

        for (var j = 0; j < workers; j++)
        {
            _coordinator.Publish(_worker, j, shard);
        }

        if (profile)
        {
            var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordSplit(_worker, t1 - t0, shard.Count);
            t0 = t1;
        }

        _coordinator.Wait(_abort);

        if (profile)
        {
            var t2 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordWait(_worker, t2 - t0);
            t0 = t2;
        }

        // Gather every worker's shard from our column into the full replica. Each
        // input row lives on one worker, so the union is a concatenation; the
        // builder still sums, harmless if an upstream ever duplicates a row.
        var gathered = new ZSetBuilder<TRow, TWeight>();
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            var bucket = _coordinator.Read(src, _worker);
            if (bucket is not null)
            {
                gathered.AddRange(bucket);
                gatherRows += bucket.Count;
            }
        }

        _output.SetCurrent(gathered.Build());

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
