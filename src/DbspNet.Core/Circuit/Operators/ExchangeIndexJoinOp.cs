// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// A <em>dual</em> all-to-all shuffle + re-index for a join's two inputs, sharing
/// <b>one</b> cross-worker rendezvous: re-partitions both the left and the right
/// Z-set stream by <c>hash(key)%W</c> (same join key, so matching rows co-locate)
/// and gathers each into an <see cref="IndexedZSet{TKey,TRow,TWeight}"/> — across a
/// single <see cref="ExchangeCoordinator{T}"/> barrier instead of the two that a
/// pair of independent <see cref="ExchangeIndexOp{TKey,TRow,TWeight}"/>s would use.
/// </summary>
/// <remarks>
/// <para>The step decomposition (docs/design-row-representation.md §15) showed the
/// scaling ceiling is barrier <b>coordination</b> — the per-exchange rendezvous
/// idles every worker until the slowest arrives, and a join emits two such
/// rendezvous back to back. Each barrier pays that straggler tax independently, so
/// q4's exchange wait reaches ~40% of the step at W=24. Fusing the two exchanges
/// into one barrier halves the join's straggler exposure: both sides are split,
/// the pair of buckets is published per destination, the workers rendezvous
/// <em>once</em>, and both columns are gathered. The join math is untouched (the
/// two indexed outputs are identical to the unfused form), and because the two
/// exchanges were independent and adjacent (no data dependency between them — see
/// <c>TypedPlanCompiler.CompileJoin</c>) one barrier is sufficient.</para>
/// <para>The shared coordinator carries a per-cell pair — the left bucket and the
/// right bucket worker <c>from</c> is sending to worker <c>to</c>. Coordination is
/// otherwise identical to <see cref="ExchangeIndexOp{TKey,TRow,TWeight}"/>: the
/// self-synchronising mailbox grid, the publish-then-<see cref="ExchangeCoordinator{T}.Wait"/>,
/// the column gather. See that type for the rendezvous contract.</para>
/// </remarks>
internal sealed class ExchangeIndexJoinOp<TKey, TLeftRow, TRightRow, TWeight> : IOperator
    where TKey : notnull
    where TLeftRow : notnull
    where TRightRow : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TLeftRow, TWeight>> _leftInput;
    private readonly Stream<ZSet<TRightRow, TWeight>> _rightInput;
    private readonly Stream<IndexedZSet<TKey, TLeftRow, TWeight>> _leftOutput;
    private readonly Stream<IndexedZSet<TKey, TRightRow, TWeight>> _rightOutput;
    private readonly Func<TLeftRow, int> _leftPartition;
    private readonly Func<TRightRow, int> _rightPartition;
    private readonly Func<TLeftRow, TKey> _leftKeyOf;
    private readonly Func<TRightRow, TKey> _rightKeyOf;
    private readonly ExchangeCoordinator<DualBucket> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeIndexJoinOp(
        Stream<ZSet<TLeftRow, TWeight>> leftInput,
        Stream<ZSet<TRightRow, TWeight>> rightInput,
        Stream<IndexedZSet<TKey, TLeftRow, TWeight>> leftOutput,
        Stream<IndexedZSet<TKey, TRightRow, TWeight>> rightOutput,
        Func<TLeftRow, int> leftPartition,
        Func<TRightRow, int> rightPartition,
        Func<TLeftRow, TKey> leftKeyOf,
        Func<TRightRow, TKey> rightKeyOf,
        ExchangeCoordinator<DualBucket> coordinator,
        int worker,
        CancellationToken abort)
    {
        _leftInput = leftInput;
        _rightInput = rightInput;
        _leftOutput = leftOutput;
        _rightOutput = rightOutput;
        _leftPartition = leftPartition;
        _rightPartition = rightPartition;
        _leftKeyOf = leftKeyOf;
        _rightKeyOf = rightKeyOf;
        _coordinator = coordinator;
        _worker = worker;
        _workers = coordinator.Workers;
        _abort = abort;
    }

    /// <summary>The pair of per-destination buckets one worker sends another in a tick.</summary>
    internal readonly record struct DualBucket(
        List<KeyValuePair<TLeftRow, TWeight>>? Left,
        List<KeyValuePair<TRightRow, TWeight>>? Right);

    public void Step()
    {
        var workers = _workers;
        var profile = StepProfiler.Enabled;
        var t0 = profile ? System.Diagnostics.Stopwatch.GetTimestamp() : 0L;

        // Split each side's rows into one append-only bucket per destination. The
        // inputs are Z-sets so their keys are distinct, so a bucket never merges
        // (see ExchangeIndexOp). Buckets are allocated lazily; empty cells stay null.
        var leftBuckets = new List<KeyValuePair<TLeftRow, TWeight>>?[workers];
        var rightBuckets = new List<KeyValuePair<TRightRow, TWeight>>?[workers];
        long splitRows = 0;
        foreach (var kv in _leftInput.Current)
        {
            var j = ((_leftPartition(kv.Key) % workers) + workers) % workers;
            (leftBuckets[j] ??= new List<KeyValuePair<TLeftRow, TWeight>>()).Add(kv);
            splitRows++;
        }

        foreach (var kv in _rightInput.Current)
        {
            var j = ((_rightPartition(kv.Key) % workers) + workers) % workers;
            (rightBuckets[j] ??= new List<KeyValuePair<TRightRow, TWeight>>()).Add(kv);
            splitRows++;
        }

        // Publish both sides' rows of the grid as one pair-payload per destination,
        // then rendezvous ONCE — the whole point of fusing the two exchanges.
        for (var j = 0; j < workers; j++)
        {
            _coordinator.Publish(_worker, j, new DualBucket(leftBuckets[j], rightBuckets[j]));
        }

        if (profile)
        {
            var t1 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordSplit(_worker, t1 - t0, splitRows);
            t0 = t1;
        }

        _coordinator.Wait(_abort);

        if (profile)
        {
            var t2 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordWait(_worker, t2 - t0);
            t0 = t2;
        }

        // Gather our column for each side straight into indexed form (each full row
        // hashed once; the builder merges a row arriving from two sources, as with
        // a single ExchangeIndex).
        var leftIndexed = new IndexedZSetBuilder<TKey, TLeftRow, TWeight>();
        var rightIndexed = new IndexedZSetBuilder<TKey, TRightRow, TWeight>();
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            var pair = _coordinator.Read(src, _worker);
            if (pair.Left is not null)
            {
                foreach (var (row, weight) in pair.Left)
                {
                    leftIndexed.Add(_leftKeyOf(row), row, weight);
                    gatherRows++;
                }
            }

            if (pair.Right is not null)
            {
                foreach (var (row, weight) in pair.Right)
                {
                    rightIndexed.Add(_rightKeyOf(row), row, weight);
                    gatherRows++;
                }
            }
        }

        _leftOutput.SetCurrent(leftIndexed.Build());
        _rightOutput.SetCurrent(rightIndexed.Build());

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
