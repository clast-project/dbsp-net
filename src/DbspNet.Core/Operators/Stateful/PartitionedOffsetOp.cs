// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>Which positional row a <see cref="OffsetSpec"/> reads from.</summary>
public enum OffsetKind
{
    /// <summary><c>LAG</c>: the row <c>Offset</c> positions before the current one.</summary>
    Lag,

    /// <summary><c>LEAD</c>: the row <c>Offset</c> positions after the current one.</summary>
    Lead,

    /// <summary><c>FIRST_VALUE</c>: the first row of the partition (UNLIMITED RANGE).</summary>
    FirstValue,

    /// <summary><c>LAST_VALUE</c>: the last row of the partition (UNLIMITED RANGE).</summary>
    LastValue,
}

/// <summary>
/// One <c>LAG</c> / <c>LEAD</c> / <c>FIRST_VALUE</c> / <c>LAST_VALUE</c> output
/// column: read <see cref="Value"/> from the row (of type
/// <typeparamref name="TRow"/>) selected by <see cref="Kind"/> (using
/// <see cref="Offset"/> for the relative LAG/LEAD kinds), falling back to
/// <see cref="Default"/> when that row is outside the partition.
/// </summary>
public readonly record struct OffsetSpec<TRow>(
    Func<TRow, object?> Value,
    OffsetKind Kind,
    long Offset,
    object? Default);

/// <summary>
/// Incremental partitioned <c>LAG</c> / <c>LEAD</c> — positional offset functions
/// emitted as new column(s) appended to every input row. The per-partition
/// generalisation of an ordered offset lookup, maintained by recompute-and-diff
/// like <see cref="PartitionedTopKOp{TRow,TKey}"/>.
/// </summary>
/// <remarks>
/// <para>Each partition keeps its integrated rows sorted by a total order
/// (<paramref name="order"/> = ORDER BY keys then a full-row tiebreak). A
/// weight-<c>w</c> row occupies <c>w</c> consecutive positions (bag semantics);
/// for each position the operator reads each spec's value from the position
/// <see cref="OffsetSpec.Offset"/> away. The whole touched partition is recomputed
/// each tick and the widened-row multiset diffed against the last emitted one —
/// inserting one row shifts only the O(offset) neighbours' results, but the
/// recompute is currently whole-partition (a neighbourhood-only recompute and
/// frontier GC are deferred).</para>
/// <para>Row-opaque: the only row-shape-specific seam is the supplied
/// <c>widen</c> delegate, which fuses the base row with the per-spec offset values
/// into the widened output row — letting the same operator run over structural or
/// typed rows.</para>
/// </remarks>
/// <typeparam name="TInRow">The input (base) row type.</typeparam>
/// <typeparam name="TOutRow">The widened output row type (base ⧺ offset columns).</typeparam>
/// <typeparam name="TKey">The PARTITION BY key type.</typeparam>
internal sealed class PartitionedOffsetOp<TInRow, TOutRow, TKey> : IOperator, ISnapshotable, IIntrospectable, ISnapshotOrder
    where TInRow : notnull
    where TOutRow : notnull
    where TKey : notnull
{
    private readonly Stream<ZSet<TInRow, Z64>> _input;
    private readonly Stream<ZSet<TOutRow, Z64>> _output;
    private readonly Func<TInRow, TKey> _partitionOf;
    private readonly IComparer<TInRow> _order;
    private readonly OffsetSpec<TInRow>[] _specs;
    private readonly Func<TInRow, object?[], TOutRow> _widen;
    private readonly IZSetTraceCodec<TInRow, Z64>? _snapshotCodec;

    private readonly Dictionary<TKey, SortedDictionary<TInRow, long>> _accum;

    // Or, under DBSPNET_TRACE_BLOCKS=window (docs/design-flat-trace-state.md §3.19), the ordered
    // store: small partitions managed, large ones as ordered chunks, routed by a numeric prefix of
    // the first ORDER BY key; a touch then reads only the neighbourhood its change reaches.
    // _accum then stays empty. Structural rows and keys only.
    private readonly Blocks.OrderedPartitionStore? _store;

    // The furthest a LAG/LEAD reads (slots); whether any spec reads the partition's ends.
    private readonly long _reach;
    private readonly bool _readsEnds;

    // Incremental checkpoints (docs/design-durable-state.md §1f, increment 3): the snapshot is
    // the integral of the input, so the change since the last checkpoint is the inputs since.
    private readonly ZSetChangeTracker<TInRow, Z64> _changes = new();

    // Lazy restore (docs/design-durable-state.md §1f, increment 5): partitions still cold, read the
    // first time a delta touches them; null when everything is in memory.
    private LazyPartitions<TKey, TInRow>? _lazy;

    /// <inheritdoc/>
    public object? OrderFor(string fileName) =>
        fileName == "trace.arrows" ? LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf) : null;

    // The emitted output is not stored: a touched partition's previous output is recomputed from
    // the partition as it stood before the delta (docs/design-flat-trace-state.md §3.11).

    // Last tick's output count, used to pre-size this tick's delta builder
    // (docs/design-row-representation.md §16.8). The emitted diff spans only
    // the touched partitions, so no cheap exact bound exists up front.
    private int _lastOutputSize;

    public PartitionedOffsetOp(
        Stream<ZSet<TInRow, Z64>> input,
        Stream<ZSet<TOutRow, Z64>> output,
        Func<TInRow, TKey> partitionOf,
        IComparer<TInRow> order,
        OffsetSpec<TInRow>[] specs,
        Func<TInRow, object?[], TOutRow> widen,
        IEqualityComparer<TKey>? partitionComparer = null,
        IZSetTraceCodec<TInRow, Z64>? snapshotCodec = null,
        Blocks.ITraceBlockFormat? chunkFormat = null,
        Func<TInRow, long>? orderPrefix = null,
        Blocks.ITraceBlockFormat? coldFormat = null)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(partitionOf);
        ArgumentNullException.ThrowIfNull(order);
        ArgumentNullException.ThrowIfNull(specs);
        ArgumentNullException.ThrowIfNull(widen);

        _input = input;
        _output = output;
        _partitionOf = partitionOf;
        _order = order;
        _specs = specs;
        _widen = widen;
        _snapshotCodec = snapshotCodec;
        _accum = new Dictionary<TKey, SortedDictionary<TInRow, long>>(partitionComparer);
        foreach (var spec in specs)
        {
            if (spec.Kind is OffsetKind.Lag or OffsetKind.Lead)
            {
                _reach = Math.Max(_reach, spec.Offset);
            }
            else
            {
                _readsEnds = true;
            }
        }

        if (chunkFormat is not null)
        {
            if (typeof(TInRow) != typeof(StructuralRow) || typeof(TKey) != typeof(StructuralRow))
            {
                throw new ArgumentException("Blocked offset state needs structural rows and partition keys.", nameof(chunkFormat));
            }

            ArgumentNullException.ThrowIfNull(orderPrefix);
            _store = new Blocks.OrderedPartitionStore(
                chunkFormat,
                row => orderPrefix(As<TInRow>(row)),
                (IComparer<StructuralRow>)(object)_order,
                partial: null,
                promoteAt: Blocks.TraceBlocksMode.PromoteRows,
                chunkRows: Blocks.TraceBlocksMode.ChunkRows,
                coldFormat: coldFormat);
        }
    }

    private static StructuralRow S<T>(T value) => (StructuralRow)(object)value!;

    private static T As<T>(StructuralRow row) => (T)(object)row;

    private void IntegrateStore(IEnumerable<(TInRow Row, long Weight)> rows) =>
        _store!.Integrate([.. rows.Select(e => (S(_partitionOf(e.Row)), S(e.Row), e.Weight))]);

    // A row of a touched neighbourhood before (Old) and after (New) the delta.
    private readonly record struct Entry(TInRow Row, long Old, long New);

    // The blocked step: integrate, then per touched partition recompute only the rows whose
    // offset values the change can reach — within _reach slots of a changed row, on either side of
    // the delta — and emit their old outputs retracted and new ones inserted (the builder cancels
    // what did not change). FIRST_VALUE/LAST_VALUE read the partition's ends: those read it whole.
    private void StepStore(ZSet<TInRow, Z64> delta, ZSetBuilder<TOutRow, Z64> builder)
    {
        var touched = new Dictionary<TKey, List<(TInRow Row, long Dw)>>(_accum.Comparer);
        foreach (var (row, dw) in delta)
        {
            var key = _partitionOf(row);
            if (_lazy is not null)
            {
                Fault(key);
            }

            if (!touched.TryGetValue(key, out var list))
            {
                touched[key] = list = new();
            }

            list.Add((row, dw.Value));
        }

        IntegrateStore(delta.Select(kv => (kv.Key, kv.Value.Value)));
        foreach (var (key, change) in touched)
        {
            RecomputeAround(builder, key, change);
        }

        _store!.EndTick();
    }

    private void RecomputeAround(ZSetBuilder<TOutRow, Z64> builder, TKey key, List<(TInRow Row, long Dw)> change)
    {
        long lo = long.MaxValue, hi = long.MinValue;
        var valueOf = _store!.ValueOfRow;
        foreach (var (row, _) in change)
        {
            var v = valueOf(S(row));
            lo = Math.Min(lo, v);
            hi = Math.Max(hi, v);
        }

        // Rows whose output can change lie within _reach slots of a change; their values lie within
        // another _reach: read 2 x _reach rows of margin (rows hold at least one slot each).
        bool atStart, atEnd;
        var current = _readsEnds
            ? _store.ReadAround(S(key), long.MinValue, long.MaxValue, long.MaxValue, out atStart, out atEnd)
            : _store.ReadAround(S(key), lo, hi, (2 * _reach) + 1, out atStart, out atEnd);

        // The union: what was read (after the delta), the delta subtracted, removed rows merged in.
        var dws = new Dictionary<TInRow, (long Dw, bool Seen)>(change.Count);
        foreach (var (row, dw) in change)
        {
            dws[row] = (dw, false);
        }

        var entries = new List<Entry>(current.Count + change.Count);
        var gone = new List<(TInRow Row, long Dw)>();
        foreach (var (row, weight, _) in current)
        {
            var r = As<TInRow>(row);
            ref var slot = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrNullRef(dws, r);
            long old = weight;
            if (!System.Runtime.CompilerServices.Unsafe.IsNullRef(ref slot))
            {
                slot.Seen = true;
                old = weight - slot.Dw;
            }

            entries.Add(new Entry(r, old, weight));
        }

        foreach (var (row, (dw, seen)) in dws)
        {
            if (!seen)
            {
                gone.Add((row, dw));
            }
        }

        if (gone.Count > 0)
        {
            gone.Sort((a, b) => _order.Compare(a.Row, b.Row));
            var merged = new List<Entry>(entries.Count + gone.Count);
            var g = 0;
            foreach (var e in entries)
            {
                while (g < gone.Count && _order.Compare(gone[g].Row, e.Row) < 0)
                {
                    merged.Add(new Entry(gone[g].Row, -gone[g].Dw, 0));
                    g++;
                }

                merged.Add(e);
            }

            for (; g < gone.Count; g++)
            {
                merged.Add(new Entry(gone[g].Row, -gone[g].Dw, 0));
            }

            entries = merged;
        }

        // Each side's slots (an entry of weight w fills w consecutive slots), and the entries whose
        // output can change: within _reach slots of a changed entry on either side.
        var oldSlots = Slots(entries, old: true);
        var newSlots = Slots(entries, old: false);
        var affected = new bool[entries.Count];
        Mark(oldSlots, entries, affected);
        Mark(newSlots, entries, affected);
        Emit(builder, entries, oldSlots, affected, atStart, atEnd, sign: -1);
        Emit(builder, entries, newSlots, affected, atStart, atEnd, sign: +1);
    }

    private static List<int> Slots(List<Entry> entries, bool old)
    {
        var slots = new List<int>(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            for (var c = old ? entries[i].Old : entries[i].New; c > 0; c--)
            {
                slots.Add(i);
            }
        }

        return slots;
    }

    private void Mark(List<int> slots, List<Entry> entries, bool[] affected)
    {
        if (_readsEnds)
        {
            Array.Fill(affected, true);
            return;
        }

        for (var j = 0; j < slots.Count; j++)
        {
            var e = entries[slots[j]];
            if (e.Old == e.New)
            {
                continue;
            }

            for (var k = Math.Max(0, j - (int)_reach); k <= Math.Min(slots.Count - 1, j + (int)_reach); k++)
            {
                affected[slots[k]] = true;
            }
        }

        // A changed entry with no slot on this side (inserted or removed) still shifts its
        // neighbours here: mark the entries adjacent to where it would sit.
        for (var i = 0; i < entries.Count; i++)
        {
            if (entries[i].Old != entries[i].New)
            {
                for (var k = Math.Max(0, i - (int)_reach - 1); k <= Math.Min(entries.Count - 1, i + (int)_reach + 1); k++)
                {
                    affected[k] = true;
                }
            }
        }
    }

    private void Emit(
        ZSetBuilder<TOutRow, Z64> builder, List<Entry> entries, List<int> slots, bool[] affected, bool atStart, bool atEnd, long sign)
    {
        for (var j = 0; j < slots.Count; j++)
        {
            if (!affected[slots[j]])
            {
                continue;
            }

            var vs = new object?[_specs.Length];
            for (var s = 0; s < _specs.Length; s++)
            {
                var spec = _specs[s];
                var src = spec.Kind switch
                {
                    OffsetKind.Lag => j - spec.Offset,
                    OffsetKind.Lead => j + spec.Offset,
                    OffsetKind.FirstValue => 0,
                    OffsetKind.LastValue => slots.Count - 1,
                    _ => j,
                };
                if (src >= 0 && src < slots.Count)
                {
                    vs[s] = spec.Value(entries[slots[(int)src]].Row);
                }
                else if ((src < 0 && atStart) || (src >= slots.Count && atEnd))
                {
                    vs[s] = spec.Default;
                }
                else
                {
                    throw new InvalidOperationException("An offset reached past the rows read for it.");
                }
            }

            builder.Add(_widen(entries[slots[j]].Row, vs), new Z64(sign));
        }
    }

    public string MetricName => "WindowOffset";

    public long RetainedRows
    {
        get
        {
            if (_store is not null)
            {
                return _store.Entries;
            }

            long n = 0;
            foreach (var bucket in _accum.Values)
            {
                n += bucket.Count;
            }

            return n;
        }
    }

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;

    public void Step()
    {
        var delta = _input.Current;
        _changes.Record(delta);
        if (_store is not null)
        {
            var storeBuilder = new ZSetBuilder<TOutRow, Z64>(_lastOutputSize);
            if (!delta.IsEmpty)
            {
                StepStore(delta, storeBuilder);
            }
            else
            {
                _store.Freeze(); // an idle tick: drop the read cache, freeze the record (§3.22)
            }

            var storeResult = storeBuilder.Build();
            _lastOutputSize = storeResult.Count;
            _output.SetCurrent(storeResult);
            return;
        }

        Dictionary<TKey, Dictionary<TInRow, long>>? touched = null;
        if (!delta.IsEmpty)
        {
            touched = new Dictionary<TKey, Dictionary<TInRow, long>>(_accum.Comparer);
            foreach (var (row, dw) in delta)
            {
                var key = _partitionOf(row);
                if (_lazy is not null)
                {
                    Fault(key);
                }

                if (!touched.TryGetValue(key, out var dws))
                {
                    touched[key] = dws = new Dictionary<TInRow, long>();
                }

                dws[row] = dw.Value;
                if (!_accum.TryGetValue(key, out var bucket))
                {
                    bucket = new SortedDictionary<TInRow, long>(_order);
                    _accum[key] = bucket;
                }

                bucket.TryGetValue(row, out var current);
                var next = current + dw.Value;
                if (next == 0)
                {
                    bucket.Remove(row);
                    if (bucket.Count == 0)
                    {
                        _accum.Remove(key);
                    }
                }
                else
                {
                    bucket[row] = next;
                }
            }
        }

        var builder = new ZSetBuilder<TOutRow, Z64>(_lastOutputSize);
        if (touched is not null)
        {
            foreach (var (key, dws) in touched)
            {
                _accum.TryGetValue(key, out var bucket);
                var newWindow = bucket is null
                    ? new Dictionary<TOutRow, long>()
                    : ComputeWindow(bucket.Select(kv => (kv.Key, kv.Value)));
                EmitDiff(builder, newWindow, ComputeWindow(Before(bucket, dws)));
            }
        }

        var result = builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
    }

    private static void EmitDiff(
        ZSetBuilder<TOutRow, Z64> builder,
        Dictionary<TOutRow, long> newWindow,
        Dictionary<TOutRow, long>? oldWindow)
    {
        foreach (var (row, weight) in newWindow)
        {
            var old = 0L;
            oldWindow?.TryGetValue(row, out old);
            if (weight != old)
            {
                builder.Add(row, new Z64(weight - old));
            }
        }

        if (oldWindow is not null)
        {
            foreach (var (row, old) in oldWindow)
            {
                if (!newWindow.ContainsKey(row))
                {
                    builder.Add(row, new Z64(-old));
                }
            }
        }
    }

    /// <summary>A partition's rows before the delta <paramref name="dws"/> landed, in the total
    /// order: the current rows with the delta subtracted, and the rows it removed merged in.</summary>
    private IEnumerable<(TInRow Row, long Weight)> Before(SortedDictionary<TInRow, long>? bucket, Dictionary<TInRow, long> dws)
    {
        var removed = dws.Keys.Where(row => bucket is null || !bucket.ContainsKey(row)).ToList();
        removed.Sort(_order);
        var r = 0;
        if (bucket is not null)
        {
            foreach (var (row, weight) in bucket)
            {
                while (r < removed.Count && _order.Compare(removed[r], row) < 0)
                {
                    yield return (removed[r], -dws[removed[r]]);
                    r++;
                }

                yield return (row, weight - dws.GetValueOrDefault(row));
            }
        }

        for (; r < removed.Count; r++)
        {
            yield return (removed[r], -dws[removed[r]]);
        }
    }

    /// <summary>Recompute one partition's widened output multiset: expand the
    /// sorted rows into positional slots (weight-aware), then for each slot read
    /// each spec's value from its offset slot.</summary>
    private Dictionary<TOutRow, long> ComputeWindow(IEnumerable<(TInRow Row, long Weight)> bucket)
    {
        // Positional slots in total order; a weight-w row fills w consecutive
        // slots. Negative-weight rows (an invalid multiset state) contribute none.
        var slots = new List<TInRow>();
        foreach (var (row, weight) in bucket)
        {
            for (var c = 0L; c < weight; c++)
            {
                slots.Add(row);
            }
        }

        var result = new Dictionary<TOutRow, long>();
        for (var j = 0; j < slots.Count; j++)
        {
            var baseRow = slots[j];
            var vs = new object?[_specs.Length];
            for (var s = 0; s < _specs.Length; s++)
            {
                var spec = _specs[s];
                var src = spec.Kind switch
                {
                    OffsetKind.Lag => j - spec.Offset,
                    OffsetKind.Lead => j + spec.Offset,
                    OffsetKind.FirstValue => 0,
                    OffsetKind.LastValue => slots.Count - 1,
                    _ => j,
                };
                vs[s] = src >= 0 && src < slots.Count ? spec.Value(slots[(int)src]) : spec.Default;
            }

            var widened = _widen(baseRow, vs);
            result[widened] = result.GetValueOrDefault(widened) + 1;
        }

        return result;
    }

    // A fresh object even when empty: the shared ZSet.Empty must never carry checkpoint tracking.
    private ZSet<TInRow, Z64> SnapshotNow() =>
        _store is not null
            ? ZSet.FromEntries(_store.All().Select(e => (As<TInRow>(e.Row), new Z64(e.Weight))))
        : _accum.Count == 0
            ? ZSet<TInRow, Z64>.NewMutable()
            : ZSet.FromEntries(_accum.SelectMany(p => p.Value.Select(kv => (kv.Key, new Z64(kv.Value)))));

    // A partition a lazily restored operator has not touched yet: read its rows from cold before
    // the delta lands. Nothing else is rebuilt: the emitted output is recomputed when needed.
    private void Fault(TKey key)
    {
        if (_lazy!.Take(key) is not { Count: > 0 } rows)
        {
            return;
        }

        if (_store is not null)
        {
            IntegrateStore(rows);
            return;
        }

        var bucket = new SortedDictionary<TInRow, long>(_order);
        foreach (var (row, weight) in rows)
        {
            bucket[row] = weight;
        }

        _accum[key] = bucket;
    }

    // Read every partition still cold (a whole-state write, or GC).
    private void PromoteAll()
    {
        if (_lazy is null)
        {
            return;
        }

        if (_store is not null)
        {
            IntegrateStore([.. _lazy.TakeAll().Select(e => (e.Row, e.Weight))]);
            _lazy = null;
            return;
        }

        foreach (var (key, row, weight) in _lazy.TakeAll())
        {
            if (!_accum.TryGetValue(key, out var bucket))
            {
                bucket = new SortedDictionary<TInRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight;
        }

        _lazy = null;
    }

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "PartitionedOffsetOp was constructed without a snapshot codec; pass one to " +
                "CircuitBuilder.PartitionedOffset to enable Snapshot.WriteAsync/ReadAsync.");
        }

        // Saved in partition order (when the partition hash is stable), so a lazy restore reads a
        // partition as one block. While partitions are still cold, the snapshot holds only those in
        // memory, and FullState reads the rest for a write that needs every row.
        if (_store is not null)
        {
            _store.DropCache();

            // Chunks are not decoded for a checkpoint that writes only the changes since the last
            // (as the window aggregate's, §3.13); FullState decodes everything for a whole write.
            var empty = ZSet<TInRow, Z64>.NewMutable();
            _changes.Attach(
                empty,
                LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
                () => { PromoteAll(); return SnapshotNow(); },
                _store.Entries + (_lazy?.ColdCount ?? 0));
            return SaveStoreAsync(writer, empty, cancellationToken);
        }

        var snapshot = SnapshotNow();
        _changes.Attach(
            snapshot,
            LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf),
            _lazy is null ? null : () => { PromoteAll(); return SnapshotNow(); },
            _lazy is null ? null : snapshot.Count + _lazy.ColdCount);
        return _snapshotCodec.SaveAsync(writer, "trace.arrows", snapshot, cancellationToken);
    }

    // The store's snapshot, and with an evicting block cache its record's backing runs (§3.7).
    private async ValueTask SaveStoreAsync(ISnapshotWriter writer, ZSet<TInRow, Z64> empty, CancellationToken cancellationToken)
    {
        await _store!.SaveBackingAsync(writer, "record.backing", cancellationToken).ConfigureAwait(false);
        await _snapshotCodec!.SaveAsync(writer, "trace.arrows", empty, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException("PartitionedOffsetOp was constructed without a snapshot codec.");
        }

        if (_snapshotCodec is IColdRowLoader<TInRow> coldLoader
            && LazyPartitions<TKey, TInRow>.PartitionOrder(_partitionOf) is not null
            && await coldLoader.LoadColdAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false) is { } cold)
        {
            // Lazily restored: partitions are read as deltas touch them (Fault).
            _accum.Clear();
            _store?.Clear();
            _lazy = new LazyPartitions<TKey, TInRow>(cold, _partitionOf, _accum.Comparer);
            _changes.Restored(cold, cold.Count);
            return;
        }

        _lazy = null;
        var loaded = await _snapshotCodec.LoadAsync(reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        _changes.Restored(loaded);
        var tRebuild = System.Diagnostics.Stopwatch.GetTimestamp();
        _accum.Clear();
        if (_store is not null)
        {
            _store.Clear();
            IntegrateStore(loaded.Where(kv => kv.Value.Value != 0).Select(kv => (kv.Key, kv.Value.Value)));
        }
        else
        foreach (var (row, weight) in loaded)
        {
            if (weight.Value == 0)
            {
                continue;
            }

            var key = _partitionOf(row);
            if (!_accum.TryGetValue(key, out var bucket))
            {
                bucket = new SortedDictionary<TInRow, long>(_order);
                _accum[key] = bucket;
            }

            bucket[row] = weight.Value;
        }

        // §10 inferred this operator's rebuild share from PartitionedWindowAggregateOp's
        // measured 2:1 split; measure it instead (docs/design-incremental-persistence.md §11).
        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddRebuild(SnapshotRestoreProfile.MsSince(tRebuild));
        }
    }

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;
}
