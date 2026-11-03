// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>What a <see cref="BlockedJoinOp"/> emits for a matching pair.</summary>
internal enum BlockedJoinOutput
{
    /// <summary>The left value's cells, then the right value's (an inner join).</summary>
    Concat,

    /// <summary>The left value alone (a semi-join: the right side only witnesses a match).</summary>
    Left,

    /// <summary>The key alone (a semi-join on whole rows, whose rows are the keys and whose values
    /// are empty, so each row is stored once).</summary>
    Key,
}

/// <summary>
/// The structural inner equi-join of <see cref="IncrementalJoinOp{TKey,TLeft,TRight,TOut,TWeight}"/>
/// with both traces held as <see cref="BlockedIndexedTrace"/>s (docs/design-flat-trace-state.md §3):
/// the same factoring, <c>out = dl ⋈ R_t + L_{t-1} ⋈ dr</c>, for a combine that concatenates the left
/// and right value rows. Because the combine is a concatenation, a trace entry read from a block
/// has its cells boxed straight into the output row (§3.0, arm B2) — no intermediate value row.
/// A residual (a non-equi ON conjunct) is evaluated first on a view of the candidate pair that
/// boxes only the cells it reads, so a rejected pair builds no row.
/// </summary>
/// <remarks>
/// Each delta is probed against the trace (never the other way round), and the probe visits every
/// level unconsolidated, which the bilinear join may: cancelling entries cancel in the output
/// builder. Snapshots go through <see cref="BlockedIndexedTrace.Materialize"/> and the operator's
/// ordinary trace codecs, with the same file names, until checkpoints write blocks raw (§2.7).
/// </remarks>
internal sealed class BlockedJoinOp : IOperator, ISnapshotable, IIntrospectable, IFingerprintIdentity
{
    /// <summary>Its snapshot is the managed structural join's, file for file (same names, v3 run
    /// chains), so either restores the other's.</summary>
    public Type FingerprintType => typeof(IncrementalJoinOp<StructuralRow, StructuralRow, StructuralRow, StructuralRow, Z64>);

    private const string LeftTraceFile = "left.arrows";
    private const string RightTraceFile = "right.arrows";

    private readonly Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> _leftIn;
    private readonly Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> _rightIn;
    private readonly Stream<ZSet<StructuralRow, Z64>> _output;
    private readonly BlockedIndexedTrace _left;
    private readonly BlockedIndexedTrace _right;
    private readonly int _leftColumns;
    private readonly int _rightColumns;
    private readonly Func<object?[], StructuralRow> _row;
    private readonly IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? _leftCodec;
    private readonly IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? _rightCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<StructuralRow, long>? _monotoneKey;
    private readonly bool _cloneDeltas = DeltaPoolMode.Enabled;
    private readonly Func<IReadOnlyList<object?>, bool>? _residual;
    private readonly Candidate? _candidate;
    private readonly BlockedJoinOutput _outputMode;
    private long _lastGcFrontier = long.MinValue;
    private int _lastOutputSize;

    public BlockedJoinOp(
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> leftIn,
        Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> rightIn,
        Stream<ZSet<StructuralRow, Z64>> output,
        ITraceBlockFormat leftFormat,
        ITraceBlockFormat rightFormat,
        Func<object?[], StructuralRow> row,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? leftCodec,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? rightCodec,
        IFrontier? frontier,
        Func<StructuralRow, long>? monotoneKey,
        Func<IReadOnlyList<object?>, bool>? residual = null,
        BlockedJoinOutput outputMode = BlockedJoinOutput.Concat)
    {
        _leftIn = leftIn;
        _rightIn = rightIn;
        _output = output;
        _left = new BlockedIndexedTrace(leftFormat, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);
        _right = new BlockedIndexedTrace(rightFormat, TraceBlocksMode.FreezeEntries, blockEntries: TraceBlocksMode.BlockEntries);
        _leftColumns = leftFormat.ValueColumns;
        _rightColumns = rightFormat.ValueColumns;
        _row = row;
        _leftCodec = leftCodec;
        _rightCodec = rightCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
        _residual = residual;
        _candidate = residual is null ? null : new Candidate(_leftColumns, _rightColumns);
        _outputMode = outputMode;
    }

    /// <summary>Total keys retained across both traces (materialises them). For GC-bound tests.</summary>
    internal int RetainedKeyCount => _left.GroupCount + _right.GroupCount;

    internal BlockedIndexedTrace LeftTrace => _left;

    internal BlockedIndexedTrace RightTrace => _right;

    public void Step()
    {
        var dl = Own(_leftIn.Current);
        var dr = Own(_rightIn.Current);
        if (dl.IsEmpty && dr.IsEmpty)
        {
            // An idle tick: freeze what waits in managed levels now, under the step's ordinary
            // (concurrent) GC, rather than at the next checkpoint, whose batch latency mode turns
            // the conversion into blocking full GCs (design §3.2).
            _left.Freeze();
            _right.Freeze();
            _output.SetCurrent(ZSet<StructuralRow, Z64>.Empty);
            CollectGarbage();
            return;
        }

        // As IncrementalJoinOp: R_t = R_{t-1} + dr before the first pass absorbs dl ⋈ dr; the left
        // trace stays at L_{t-1} for the second pass.
        _right.Integrate(dr);
        var builder = new ZSetBuilder<StructuralRow, Z64>(_lastOutputSize);
        foreach (var (key, leftGroup) in dl)
        {
            var visitor = new DeltaOnLeft(this, key, leftGroup, builder);
            _right.Probe(key, ref visitor);
        }

        foreach (var (key, rightGroup) in dr)
        {
            var visitor = new DeltaOnRight(this, key, rightGroup, builder);
            _left.Probe(key, ref visitor);
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
        _left.Integrate(dl);
        CollectGarbage();
    }

    // The trace keeps the delta; a pooled one is reused next tick, so take a copy.
    private IndexedZSet<StructuralRow, StructuralRow, Z64> Own(IndexedZSet<StructuralRow, StructuralRow, Z64> delta)
    {
        if (!_cloneDeltas || delta.IsEmpty)
        {
            return delta;
        }

        var copy = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        foreach (var (key, group) in delta)
        {
            foreach (var (value, weight) in group)
            {
                copy.Add(key, value, weight);
            }
        }

        return copy.Build();
    }

    private void CollectGarbage()
    {
        if (_frontier is null || _monotoneKey is null)
        {
            return;
        }

        var frontier = _frontier.Value;
        if (frontier == long.MinValue || frontier <= _lastGcFrontier)
        {
            return;
        }

        _lastGcFrontier = frontier;
        _left.DropKeysBelow(frontier, _monotoneKey);
        _right.DropKeysBelow(frontier, _monotoneKey);
    }

    private static void Copy(StructuralRow row, object?[] into, int at)
    {
        var n = row.Count;
        for (var c = 0; c < n; c++)
        {
            into[at + c] = row[c];
        }
    }

    // A pair's output row when the left value is a managed row.
    private StructuralRow Emit(StructuralRow key, StructuralRow left, StructuralRow? right, ITraceBlock? rightBlock, int rightIndex)
    {
        switch (_outputMode)
        {
            case BlockedJoinOutput.Key:
                return key;
            case BlockedJoinOutput.Left:
                return left;
            default:
                var cells = new object?[_leftColumns + _rightColumns];
                Copy(left, cells, 0);
                if (right is not null)
                {
                    Copy(right, cells, _leftColumns);
                }
                else
                {
                    rightBlock!.ValueCellsInto(rightIndex, cells, _leftColumns);
                }

                return _row(cells);
        }
    }

    // A pair's output row when the left value is a block entry.
    private StructuralRow Emit(StructuralRow key, ITraceBlock leftBlock, int leftIndex, StructuralRow right)
    {
        switch (_outputMode)
        {
            case BlockedJoinOutput.Key:
                return key;
            case BlockedJoinOutput.Left:
                var left = new object?[_leftColumns];
                leftBlock.ValueCellsInto(leftIndex, left, 0);
                return _row(left);
            default:
                var cells = new object?[_leftColumns + _rightColumns];
                leftBlock.ValueCellsInto(leftIndex, cells, 0);
                Copy(right, cells, _leftColumns);
                return _row(cells);
        }
    }

    // dl ⋈ R_t: the delta's rows on the left of the output, the trace's on the right. A residual
    // is evaluated on the candidate view first, so a rejected pair builds nothing.
    private readonly struct DeltaOnLeft(BlockedJoinOp op, StructuralRow key, ZSet<StructuralRow, Z64> delta, ZSetBuilder<StructuralRow, Z64> output)
        : ITraceVisitor
    {
        public void Row(StructuralRow value, Z64 weight)
        {
            op._candidate?.Right(value);
            foreach (var (left, leftWeight) in delta)
            {
                if (op._residual is { } residual && !residual(op._candidate!.Left(left)))
                {
                    continue;
                }

                output.Add(op.Emit(key, left, value, null, 0), Z64.Multiply(leftWeight, weight));
            }
        }

        public void Entry(ITraceBlock block, int index, Z64 weight)
        {
            op._candidate?.Right(block, index);
            foreach (var (left, leftWeight) in delta)
            {
                if (op._residual is { } residual && !residual(op._candidate!.Left(left)))
                {
                    continue;
                }

                output.Add(op.Emit(key, left, null, block, index), Z64.Multiply(leftWeight, weight));
            }
        }
    }

    // L_{t-1} ⋈ dr: the trace's rows on the left, the delta's on the right.
    private readonly struct DeltaOnRight(BlockedJoinOp op, StructuralRow key, ZSet<StructuralRow, Z64> delta, ZSetBuilder<StructuralRow, Z64> output)
        : ITraceVisitor
    {
        public void Row(StructuralRow value, Z64 weight)
        {
            op._candidate?.Left(value);
            foreach (var (right, rightWeight) in delta)
            {
                if (op._residual is { } residual && !residual(op._candidate!.Right(right)))
                {
                    continue;
                }

                output.Add(op.Emit(key, value, right, null, 0), Z64.Multiply(weight, rightWeight));
            }
        }

        public void Entry(ITraceBlock block, int index, Z64 weight)
        {
            if (op._outputMode != BlockedJoinOutput.Concat && op._residual is null)
            {
                // The left row is the same for every right row: build it once.
                var row = op.Emit(key, block, index, null!);
                var total = Z64.Zero;
                foreach (var (_, rightWeight) in delta)
                {
                    total = Z64.Add(total, Z64.Multiply(weight, rightWeight));
                }

                if (!Z64.IsZero(total))
                {
                    output.Add(row, total);
                }

                return;
            }

            op._candidate?.Left(block, index);
            foreach (var (right, rightWeight) in delta)
            {
                if (op._residual is { } residual && !residual(op._candidate!.Right(right)))
                {
                    continue;
                }

                output.Add(op.Emit(key, block, index, right), Z64.Multiply(weight, rightWeight));
            }
        }
    }

    /// <summary>
    /// The output row a candidate pair would make, as a view: a cell is read (and, from a block,
    /// boxed) only when the residual asks for it. One instance per operator, repointed per pair;
    /// a compiled predicate reads cells through <see cref="IReadOnlyList{T}"/>'s indexer and keeps
    /// nothing, so reuse is safe.
    /// </summary>
    private sealed class Candidate(int leftColumns, int rightColumns) : IReadOnlyList<object?>
    {
        private StructuralRow? _leftRow;
        private ITraceBlock? _leftBlock;
        private int _leftIndex;
        private StructuralRow? _rightRow;
        private ITraceBlock? _rightBlock;
        private int _rightIndex;

        public int Count => leftColumns + rightColumns;

        public object? this[int index] => index < leftColumns
            ? _leftRow is { } l ? l[index] : _leftBlock!.ValueCell(_leftIndex, index)
            : _rightRow is { } r ? r[index - leftColumns] : _rightBlock!.ValueCell(_rightIndex, index - leftColumns);

        public Candidate Left(StructuralRow row)
        {
            (_leftRow, _leftBlock) = (row, null);
            return this;
        }

        public Candidate Left(ITraceBlock block, int index)
        {
            (_leftRow, _leftBlock, _leftIndex) = (null, block, index);
            return this;
        }

        public Candidate Right(StructuralRow row)
        {
            (_rightRow, _rightBlock) = (row, null);
            return this;
        }

        public Candidate Right(ITraceBlock block, int index)
        {
            (_rightRow, _rightBlock, _rightIndex) = (null, block, index);
            return this;
        }

        public IEnumerator<object?> GetEnumerator()
        {
            for (var i = 0; i < Count; i++)
            {
                yield return this[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_leftCodec is null || _rightCodec is null)
        {
            throw new NotSupportedException("BlockedJoinOp was constructed without snapshot codecs.");
        }

        await Save(_leftCodec, LeftTraceFile, _left).ConfigureAwait(false);
        await Save(_rightCodec, RightTraceFile, _right).ConfigureAwait(false);

        ValueTask Save(IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, string file, BlockedIndexedTrace trace) =>
            codec is IBlockedTraceCodec blocked
                ? blocked.SaveBlockedAsync(writer, file, trace, cancellationToken)
                : codec.SaveAsync(writer, file, trace.Materialize(), cancellationToken);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_leftCodec is null || _rightCodec is null)
        {
            throw new NotSupportedException("BlockedJoinOp was constructed without snapshot codecs.");
        }

        await Load(_leftCodec, LeftTraceFile, _left).ConfigureAwait(false);
        await Load(_rightCodec, RightTraceFile, _right).ConfigureAwait(false);

        async ValueTask Load(IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> codec, string file, BlockedIndexedTrace trace)
        {
            if (codec is IBlockedTraceCodec blocked)
            {
                await blocked.LoadBlockedAsync(reader, file, trace, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                trace.Integrate(await codec.LoadAsync(reader, file, cancellationToken).ConfigureAwait(false));
            }
        }
    }

    public string SchemaFingerprint =>
        $"left={_leftCodec?.SchemaFingerprint ?? ""};right={_rightCodec?.SchemaFingerprint ?? ""}";

    public string MetricName => "IncrementalInnerJoin";

    public long RetainedRows => RetainedKeyCount;

    public long LastOutputRows => (_output.Current?.Count ?? 0);

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _left.DroppedKeys + _right.DroppedKeys;
}
