// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// A fused all-to-all shuffle + re-index: re-partitions a sharded Z-set stream
/// by <c>hash(key) % W</c> (so every row for a given key is co-located on one
/// worker) and, in the same pass, groups the gathered rows by that key into an
/// <see cref="IndexedZSet{TKey,TRow,TWeight}"/> ready for a join / aggregate.
/// </summary>
/// <remarks>
/// Equivalent to an <see cref="ExchangeOp{TKey,TWeight}"/> immediately followed
/// by a <c>GroupProject(keyOf, identity)</c>, but it skips the intermediate flat
/// Z-set: the plain exchange's gather builds a <see cref="ZSet{TRow,TWeight}"/>
/// (hashing every full row) only for the re-index to immediately rebuild it as an
/// indexed Z-set (hashing every full row again). Building the index directly at
/// the gather hashes each full row once instead of twice and never allocates the
/// throwaway flat Z-set. Coordination is identical to <see cref="ExchangeOp{TKey,
/// TWeight}"/> — the same self-synchronizing per-tick mailbox grid and barrier;
/// see that type for the rendezvous contract.
/// </remarks>
internal sealed class ExchangeIndexOp<TKey, TRow, TWeight> : IOperator
    where TKey : notnull
    where TRow : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TRow, TWeight>> _input;
    private readonly Stream<IndexedZSet<TKey, TRow, TWeight>> _output;
    private readonly Func<TRow, int> _partition;
    private readonly Func<TRow, TKey> _keyOf;
    private readonly ExchangeCoordinator<List<KeyValuePair<TRow, TWeight>>> _coordinator;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;

    internal ExchangeIndexOp(
        Stream<ZSet<TRow, TWeight>> input,
        Stream<IndexedZSet<TKey, TRow, TWeight>> output,
        Func<TRow, int> partition,
        Func<TRow, TKey> keyOf,
        ExchangeCoordinator<List<KeyValuePair<TRow, TWeight>>> coordinator,
        int worker,
        CancellationToken abort)
    {
        _input = input;
        _output = output;
        _partition = partition;
        _keyOf = keyOf;
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

        // Split this shard's rows into one append-only bucket per destination —
        // the input is a Z-set so its keys are distinct, so a bucket never
        // merges (see ExchangeOp). Buckets are allocated lazily; empty cells
        // publish null.
        var buckets = new List<KeyValuePair<TRow, TWeight>>?[workers];
        long splitRows = 0;
        foreach (var kv in _input.Current)
        {
            // Non-negative modulo: partition() may return a negative hash.
            var j = ((_partition(kv.Key) % workers) + workers) % workers;
            (buckets[j] ??= new List<KeyValuePair<TRow, TWeight>>()).Add(kv);
            splitRows++;
        }

        for (var j = 0; j < workers; j++)
        {
            _coordinator.Publish(_worker, j, buckets[j]!);
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

        // Gather our column straight into the indexed form. Building the index
        // here (rather than a flat Z-set re-indexed downstream) is the whole
        // point: each full row is hashed once. The builder accumulates weight
        // per (key, row) and drops zeros, so the same row arriving from two
        // source workers (a column-dropping map upstream) merges correctly.
        var indexed = new IndexedZSetBuilder<TKey, TRow, TWeight>();
        long gatherRows = 0;
        for (var src = 0; src < workers; src++)
        {
            var bucket = _coordinator.Read(src, _worker);
            if (bucket is not null)
            {
                foreach (var (row, weight) in bucket)
                {
                    indexed.Add(_keyOf(row), row, weight);
                    gatherRows++;
                }
            }
        }

        _output.SetCurrent(indexed.Build());

        if (profile)
        {
            var t3 = System.Diagnostics.Stopwatch.GetTimestamp();
            StepProfiler.RecordGather(_worker, t3 - t0, gatherRows);
        }
    }
}
