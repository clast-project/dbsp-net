// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// A global rank (<c>ROW_NUMBER</c> / <c>RANK</c> with no <c>PARTITION BY</c>) spread over the workers by
/// ranges of its ORDER BY keys (docs/design-bounded-memory.md §5.50). Worker w owns range w and ranks
/// its rows with a private single-partition <see cref="PartitionedRankOp{TInRow,TOutRow,TKey}"/>; a
/// row's rank is its rank in the range plus the rows of every range before it. The result is right for
/// any range boundaries; the boundaries only decide how evenly the work spreads.
/// </summary>
/// <remarks>
/// <para>One step, three rendezvous: (1) every worker publishes its state's row count and its delta's
/// size, so every replica knows the old range counts (hence the old offsets) and decides alike whether
/// to re-split; (2) on a re-split only, every worker publishes a sample of its delta and state, and
/// every replica picks the same splitters from the merged sample; every worker then gives up its rows
/// and retracts its output; (3) the rows are routed to their ranges' owners, each cell carrying the
/// sender's net weight per destination, so every replica learns every range's new count.</para>
/// <para>Splitters compare the ORDER BY keys alone, so a tie group never straddles two ranges: RANK's
/// offset is then the rows before the range, as ROW_NUMBER's. DENSE_RANK would need each range's
/// distinct groups, which routing cannot count, and is not supported.</para>
/// <para>A re-split happens when the rows (with this tick's delta) reach <c>minRows</c> and twice the
/// rows of the last split. Before the first split there are no splitters: one range, on worker 0,
/// which is the gathered rank exactly. A re-split moves every row and re-emits every output row; the
/// doubling keeps the total moved below twice the final size.</para>
/// <para>Checkpoints: the private rank's rows (<c>trace.arrows</c>), the splitters
/// (<c>splitters.arrows</c>) and a small <see cref="MetaFile"/> (W, the rows at the last split, this
/// worker's row count). A re-shard sends every row to worker 0 and drops the other two files: the
/// operator then starts again with one range, which the next step re-splits.</para>
/// </remarks>
internal sealed class RangeRankOp : IOperator, ISnapshotable, IIntrospectable, ISnapshotOrder
{
    /// <summary>The checkpoint file holding W, the rows at the last split and this worker's rows.</summary>
    internal const string MetaFile = "range.meta";

    /// <summary>The checkpoint file holding the splitters (weight = position + 1).</summary>
    internal const string SplittersFile = "splitters.arrows";

    /// <summary>Rows each worker samples for a re-split.</summary>
    internal const int SampleRows = 1_024;

    private const long MetaVersion = 1;

    private readonly Stream<ZSet<StructuralRow, Z64>> _input;
    private readonly Stream<ZSet<StructuralRow, Z64>> _output;
    private readonly Stream<ZSet<StructuralRow, Z64>> _localIn;
    private readonly Stream<ZSet<StructuralRow, Z64>> _localOut;
    private readonly PartitionedRankOp<StructuralRow, StructuralRow, StructuralRow> _local;
    private readonly IComparer<StructuralRow> _order;
    private readonly IComparer<StructuralRow> _sortKeyOnly;
    private readonly IZSetTraceCodec<StructuralRow, Z64>? _codec;
    private readonly ExchangeCoordinator<long[]> _sizes;
    private readonly ExchangeCoordinator<(List<StructuralRow> Rows, long Population)> _samples;
    private readonly ExchangeCoordinator<(List<KeyValuePair<StructuralRow, Z64>>? Rows, long[] Nets)> _route;
    private readonly int _worker;
    private readonly int _workers;
    private readonly CancellationToken _abort;
    private readonly long _minRows;

    // Range r is owned by worker r; a row's range is the number of splitters at or below it (by the
    // ORDER BY keys). Strictly increasing, at most W − 1 of them; empty = one range, on worker 0.
    private StructuralRow[] _splitters = [];

    // Total rows when the splitters were last chosen.
    private long _rowsAtSplit;

    // This worker's rows (the sum of its range's weights).
    private long _localRows;

    // Restored without a count (a re-shard dropped the meta file): recount before the next step.
    private bool _recount;

    internal RangeRankOp(
        Stream<ZSet<StructuralRow, Z64>> input,
        Stream<ZSet<StructuralRow, Z64>> output,
        Func<StructuralRow, StructuralRow> partitionOf,
        IComparer<StructuralRow> order,
        IComparer<StructuralRow> sortKeyOnly,
        RankFunction function,
        Func<StructuralRow, long, StructuralRow> widen,
        IZSetTraceCodec<StructuralRow, Z64>? codec,
        ExchangeCoordinator<long[]> sizes,
        ExchangeCoordinator<(List<StructuralRow> Rows, long Population)> samples,
        ExchangeCoordinator<(List<KeyValuePair<StructuralRow, Z64>>? Rows, long[] Nets)> route,
        int worker,
        long minRows,
        CancellationToken abort)
    {
        if (function == RankFunction.DenseRank)
        {
            throw new ArgumentException("A range rank cannot number DENSE_RANK.", nameof(function));
        }

        _input = input;
        _output = output;
        _order = order;
        _sortKeyOnly = sortKeyOnly;
        _codec = codec;
        _sizes = sizes;
        _samples = samples;
        _route = route;
        _worker = worker;
        _workers = sizes.Workers;
        _abort = abort;
        _minRows = minRows;
        _localIn = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
        _localOut = new Stream<ZSet<StructuralRow, Z64>>(ZSet<StructuralRow, Z64>.Empty);
        _local = new PartitionedRankOp<StructuralRow, StructuralRow, StructuralRow>(
            _localIn, _localOut, partitionOf, order, sortKeyOnly, function, widen, null, codec);
    }

    /// <summary>Re-splits this operator has made (tests, diagnostics).</summary>
    internal long Resplits { get; private set; }

    /// <summary>The current splitters (tests).</summary>
    internal IReadOnlyList<StructuralRow> Splitters => _splitters;

    /// <summary>This worker's rows (tests).</summary>
    internal long LocalRows => _localRows;

    public string MetricName => "RangeRank";

    public long RetainedRows => _local.RetainedRows;

    public long LastOutputRows => _output.LastCount;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public object? OrderFor(string fileName) => _local.OrderFor(fileName);

    public string SchemaFingerprint => _local.SchemaFingerprint;

    public void Step()
    {
        if (_recount)
        {
            _localRows = _local.TotalWeight();
            _recount = false;
        }

        var delta = _input.Current;
        var workers = _workers;

        // (1) Sizes: every worker's rows (the old range counts) and its delta's size, with the split
        // state every replica must share: a replica that disagreed would wait at a different barrier
        // than the rest, so a disagreement throws instead.
        var mine = new[] { _localRows, delta.Count, _rowsAtSplit, _splitters.Length };
        for (var j = 0; j < workers; j++)
        {
            _sizes.Publish(_worker, j, mine);
        }

        _sizes.Wait(_abort);
        var oldCounts = new long[workers];
        long total = 0;
        for (var src = 0; src < workers; src++)
        {
            var s = _sizes.Read(src, _worker);
            oldCounts[src] = s[0];
            total += s[0] + s[1];
            if (s[2] != _rowsAtSplit || s[3] != _splitters.Length)
            {
                throw new InvalidOperationException(
                    $"range rank replicas disagree: worker {src} split at {s[2]} rows into {s[3] + 1} ranges, worker {_worker} at {_rowsAtSplit} into {_splitters.Length + 1}");
            }
        }

        var resplit = total >= _minRows && total >= 2 * _rowsAtSplit && total > 0;
        long oldOffset = 0;
        for (var src = 0; src < _worker; src++)
        {
            oldOffset += oldCounts[src];
        }

        ZSetBuilder<StructuralRow, Z64>? retract = null;
        List<(StructuralRow Row, long Weight)>? moved = null;
        if (resplit)
        {
            // (2) Samples, then splitters every replica computes alike; this worker's rows leave it.
            Publish(Sample(delta));
            _samples.Wait(_abort);
            _splitters = ChooseSplitters();
            _local.SetOffsets(oldOffset, oldOffset);
            retract = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(int.MaxValue, _localRows));
            moved = _local.Evict(retract);
            Resplits++;
        }

        // (3) Route the delta (and the moved rows) to their ranges; every cell carries the sender's
        // net weight for each destination.
        var buckets = new List<KeyValuePair<StructuralRow, Z64>>?[workers];
        var nets = new long[workers];
        void Send(StructuralRow row, Z64 weight)
        {
            var r = RangeOf(row);
            (buckets[r] ??= []).Add(new KeyValuePair<StructuralRow, Z64>(row, weight));
            nets[r] += weight.Value;
        }

        foreach (var (row, weight) in delta)
        {
            Send(row, weight);
        }

        if (moved is not null)
        {
            foreach (var (row, weight) in moved)
            {
                Send(row, new Z64(weight));
            }
        }

        for (var j = 0; j < workers; j++)
        {
            _route.Publish(_worker, j, (buckets[j], nets));
        }

        _route.Wait(_abort);
        var newCounts = resplit ? new long[workers] : (long[])oldCounts.Clone();
        long rows = 0;
        var cells = new List<KeyValuePair<StructuralRow, Z64>>?[workers];
        for (var src = 0; src < workers; src++)
        {
            var (bucket, senderNets) = _route.Take(src, _worker);
            cells[src] = bucket;
            rows += bucket?.Count ?? 0;
            for (var j = 0; j < workers; j++)
            {
                newCounts[j] += senderNets[j];
            }
        }

        var gathered = new ZSetBuilder<StructuralRow, Z64>((int)Math.Min(rows, int.MaxValue));
        foreach (var bucket in cells)
        {
            if (bucket is not null)
            {
                gathered.AddRange(bucket);
            }
        }

        long newOffset = 0;
        for (var src = 0; src < _worker; src++)
        {
            newOffset += newCounts[src];
        }

        if (resplit)
        {
            _rowsAtSplit = newCounts.Sum();
            oldOffset = newOffset; // the evicted rank is empty: nothing old to shift
        }

        _localRows = newCounts[_worker];
        _local.SetOffsets(oldOffset, newOffset);
        _localIn.SetCurrent(gathered.Build());
        _local.Step();
        var ranked = _localOut.Current;
        if (retract is null)
        {
            _output.SetCurrent(ranked);
            return;
        }

        retract.AddRange(ranked);
        _output.SetCurrent(retract.Build());
    }

    private void Publish((List<StructuralRow> Rows, long Population) sample)
    {
        for (var j = 0; j < _workers; j++)
        {
            _samples.Publish(_worker, j, sample);
        }
    }

    // Up to SampleRows of this worker's inserted rows and its state, uniformly (a reservoir), with the
    // number of rows they stand for. Need not be deterministic: every replica reads the same samples.
    private (List<StructuralRow> Rows, long Population) Sample(ZSet<StructuralRow, Z64> delta)
    {
        var rng = new Random(HashCode.Combine(_worker, _rowsAtSplit));
        var sample = new List<StructuralRow>(SampleRows);
        long seen = 0;
        void Offer(StructuralRow row)
        {
            seen++;
            if (sample.Count < SampleRows)
            {
                sample.Add(row);
            }
            else if (rng.NextInt64(seen) is var k && k < SampleRows)
            {
                sample[(int)k] = row;
            }
        }

        foreach (var (row, weight) in delta)
        {
            if (weight.Value > 0)
            {
                Offer(row);
            }
        }

        foreach (var (row, _) in _local.Rows())
        {
            Offer(row);
        }

        return (sample, seen);
    }

    // The same splitters on every replica: the samples in worker order, each row standing for its
    // worker's population over its sample size, sorted by the total order; W − 1 weighted quantiles,
    // keeping only those strictly above the previous one by the ORDER BY keys.
    private StructuralRow[] ChooseSplitters()
    {
        var merged = new List<(StructuralRow Row, double Weight)>();
        double total = 0;
        for (var src = 0; src < _workers; src++)
        {
            var (rows, population) = _samples.Read(src, _worker);
            if (rows.Count == 0)
            {
                continue;
            }

            var each = (double)population / rows.Count;
            foreach (var row in rows)
            {
                merged.Add((row, each));
            }

            total += population;
        }

        merged.Sort((a, b) => _order.Compare(a.Row, b.Row));
        var splitters = new List<StructuralRow>(_workers - 1);
        double cumulative = 0;
        var next = 1;
        foreach (var (row, weight) in merged)
        {
            cumulative += weight;
            while (next < _workers && cumulative >= next * total / _workers)
            {
                if (splitters.Count == 0 || _sortKeyOnly.Compare(splitters[^1], row) < 0)
                {
                    splitters.Add(row);
                }

                next++;
            }
        }

        // A splitter at or below every row would leave range 0 empty and move nothing: drop the first if
        // it equals the smallest row's keys.
        if (splitters.Count > 0 && merged.Count > 0 && _sortKeyOnly.Compare(splitters[0], merged[0].Row) <= 0)
        {
            splitters.RemoveAt(0);
        }

        return [.. splitters];
    }

    private int RangeOf(StructuralRow row)
    {
        var splitters = _splitters;
        int lo = 0, hi = splitters.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (_sortKeyOnly.Compare(splitters[mid], row) <= 0)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid;
            }
        }

        return lo;
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_codec is null)
        {
            throw new NotSupportedException("RangeRankOp was constructed without a snapshot codec.");
        }

        await _local.SaveAsync(writer, cancellationToken).ConfigureAwait(false);
        var splitters = _splitters.Length == 0
            ? ZSet<StructuralRow, Z64>.NewMutable()
            : ZSet.FromEntries(_splitters.Select((row, i) => (row, new Z64(i + 1))));

        await _codec.SaveAsync(writer, SplittersFile, splitters, cancellationToken).ConfigureAwait(false);
        var buffer = new byte[32];
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(0), MetaVersion);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(8), _workers);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(16), _rowsAtSplit);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(24), _recount ? _local.TotalWeight() : _localRows);
        await using var file = await writer.CreateAsync(MetaFile, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        await stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_codec is null)
        {
            throw new NotSupportedException("RangeRankOp was constructed without a snapshot codec.");
        }

        await _local.LoadAsync(reader, cancellationToken).ConfigureAwait(false);
        _local.SetOffsets(0, 0);
        _splitters = [];
        _rowsAtSplit = 0;
        _localRows = 0;
        _recount = true;
        if (!await reader.ExistsAsync(MetaFile, cancellationToken).ConfigureAwait(false))
        {
            // Re-sharded: every row is on worker 0, one range; the next step re-splits.
            return;
        }

        var buffer = new byte[32];
        await using (var file = await reader.OpenReadAsync(MetaFile, cancellationToken).ConfigureAwait(false))
        await using (var stream = file.AsStream())
        {
            await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
        }

        var version = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(0));
        var workers = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(8));
        if (version != MetaVersion || workers != _workers)
        {
            throw new InvalidDataException(
                $"range rank checkpoint written for W={workers} (version {version}); this circuit has W={_workers} and was not re-sharded");
        }

        var splitters = await _codec.LoadAsync(reader, SplittersFile, cancellationToken).ConfigureAwait(false);
        _splitters = [.. splitters.Where(kv => kv.Value.Value > 0).OrderBy(kv => kv.Value.Value).Select(kv => kv.Key)];
        _rowsAtSplit = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(16));
        _localRows = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(24));
        _recount = false;
    }
}
