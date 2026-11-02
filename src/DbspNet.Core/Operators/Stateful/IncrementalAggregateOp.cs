// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Per-group incremental aggregate. State: an integrated indexed Z-set of
/// value multisets (one per group key), plus a cache of the last-emitted
/// aggregate value per key and an opaque per-key scratch state that the
/// aggregator uses to incrementalize its computation. Per tick:
/// <list type="number">
///   <item>For each group key touched by the delta, hand the aggregator its
///         prior cached value, the per-key delta, and the post-delta
///         multiset. The aggregator either folds the delta into its state
///         (SUM, COUNT, AVG) or rescans the post-delta multiset (MIN, MAX).</item>
///   <item>If the aggregate result changed, emit a retraction of the old
///         (key, value) pair and an insertion of the new one.</item>
///   <item>Drop cache entries for groups that became empty.</item>
/// </list>
/// </summary>
internal sealed class IncrementalAggregateOp<TKey, TValue, TOut> : IOperator, ISnapshotable, IIntrospectable
    where TKey : notnull
    where TValue : notnull
    where TOut : notnull
{
    private readonly Stream<IndexedZSet<TKey, TValue, Z64>> _input;
    private readonly Stream<ZSet<(TKey Key, TOut Value), Z64>> _output;

    // Previous tick's output size — pre-sizes this tick's delta builder to avoid
    // dictionary-resize churn (fresh alloc, no reuse; §16.7). Perf hint only.
    private int _lastOutputSize;

    // When DeltaPoolMode is enabled at construction, reuse one output builder
    // across ticks (Reset + refill + BuildShared) instead of allocating a fresh
    // dictionary every Step — the §20 cross-tick delta pooling. Null = the
    // default fresh-builder-per-tick path. Sound only on a dead-after-tick edge
    // (no z⁻¹/terminal retains the output); see DeltaPoolMode.
    private readonly ZSetBuilder<(TKey, TOut), Z64>? _pooledBuilder =
        DeltaPoolMode.Enabled ? new ZSetBuilder<(TKey, TOut), Z64>() : null;

    private readonly IAggregator<TValue, TOut> _aggregator;
    private readonly IGroupTrace<TKey, TValue, Z64> _trace;
    private readonly Dictionary<TKey, Optional<TOut>> _aggCache = new();
    private readonly Dictionary<TKey, object?> _stateCache = new();

    // Lazy restore (docs/design-durable-state.md §1f, increment 4): when the trace comes back cold,
    // the per-group caches are rebuilt one key at a time, the first time a delta touches it, instead
    // of from a scan of the whole trace at load. _cachesBuilt holds the keys already rebuilt (null:
    // eager); _pendingBlobs the persisted states not yet applied, keyed by group.
    private HashSet<TKey>? _cachesBuilt;
    private Dictionary<TKey, byte[]>? _pendingBlobs;
    private readonly IIndexedZSetTraceCodec<TKey, TValue, Z64>? _snapshotCodec;
    private readonly IFrontier? _frontier;
    private readonly Func<TKey, long>? _monotoneKey;
    private long _lastGcFrontier = long.MinValue;
    private long _gcDropped;

    public IncrementalAggregateOp(
        Stream<IndexedZSet<TKey, TValue, Z64>> input,
        Stream<ZSet<(TKey Key, TOut Value), Z64>> output,
        IAggregator<TValue, TOut> aggregator,
        IIndexedZSetTraceCodec<TKey, TValue, Z64>? snapshotCodec = null,
        IFrontier? frontier = null,
        Func<TKey, long>? monotoneKey = null,
        IGroupTrace<TKey, TValue, Z64>? trace = null)
    {
        _trace = trace ?? new ManagedGroupTrace<TKey, TValue, Z64>();
        _input = input;
        _output = output;
        _aggregator = aggregator;
        _snapshotCodec = snapshotCodec;
        _frontier = frontier;
        _monotoneKey = monotoneKey;
    }

    /// <summary>
    /// Number of group keys currently retained in the trace. Exposed for tests
    /// that assert frontier-driven GC keeps state bounded.
    /// </summary>
    internal int RetainedGroupCount => _trace.GroupCount;

    public ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalAggregateOp was constructed without a snapshot codec; " +
                "pass one to CircuitBuilder.IncrementalAggregate to enable Snapshot.WriteAsync/ReadAsync.");
        }

        return SaveTraceAndStateAsync(writer, cancellationToken);
    }

    // The trace alone is not enough. Rebuilding the per-group aggregator state by folding
    // the restored trace is lossy for a non-associative accumulator (float SUM/AVG/STDDEV),
    // and the operator would then retract a value the downstream view never held — see
    // docs/design-incremental-persistence.md §7.2. So when the aggregator asks for it, and
    // the codec can address blobs by key, each group's state rides alongside the trace.
    private async ValueTask SaveTraceAndStateAsync(
        ISnapshotWriter writer, CancellationToken cancellationToken)
    {
        await _trace.SaveAsync(_snapshotCodec!, writer, "trace.arrows", cancellationToken)
            .ConfigureAwait(false);

        if (!_aggregator.CanPersistState || !_snapshotCodec!.SupportsKeyedBlobs)
        {
            return;
        }

        var entries = new List<(TKey Key, byte[] Blob)>(_stateCache.Count + (_pendingBlobs?.Count ?? 0));
        var scratch = new System.Buffers.ArrayBufferWriter<byte>();
        foreach (var (key, state) in _stateCache)
        {
            scratch.Clear();
            _aggregator.WriteState(scratch, state);
            entries.Add((key, scratch.WrittenSpan.ToArray()));
        }

        // Groups a lazily restored operator has not touched keep the state they were restored with.
        if (_pendingBlobs is not null)
        {
            foreach (var (key, blob) in _pendingBlobs)
            {
                entries.Add((key, blob));
            }
        }

        await _snapshotCodec.SaveKeyedBlobsAsync(writer, "aggstate.arrows", entries, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        if (_snapshotCodec is null)
        {
            throw new NotSupportedException(
                "IncrementalAggregateOp was constructed without a snapshot codec.");
        }

        await _trace.LoadAsync(_snapshotCodec, reader, "trace.arrows", cancellationToken).ConfigureAwait(false);
        var tRebuild = System.Diagnostics.Stopwatch.GetTimestamp();

        // Lazily restored (a cold managed trace, or any blocked one): caches are rebuilt per group.
        if (_trace.IsLazy)
        {
            // Lazily restored: rebuild per group on first touch (EnsureCaches), keeping the
            // persisted states until then.
            _aggCache.Clear();
            _stateCache.Clear();
            _cachesBuilt = new HashSet<TKey>();
            _pendingBlobs = null;
            if (_aggregator.CanPersistState && _snapshotCodec.SupportsKeyedBlobs)
            {
                _pendingBlobs = new Dictionary<TKey, byte[]>();
                foreach (var (key, blob) in await _snapshotCodec
                    .LoadKeyedBlobsAsync(reader, "aggstate.arrows", cancellationToken).ConfigureAwait(false))
                {
                    _pendingBlobs[key] = blob;
                }
            }

            return;
        }

        _cachesBuilt = null;
        _pendingBlobs = null;

        // Rebuild the per-group caches from the restored trace. Each
        // aggregator's Update is already the function that maps a
        // multiset → (state, value); calling it with delta = afterMultiset
        // = group and a fresh state recovers both, matching the
        // steady-state invariants of every shipped aggregator (SUM/COUNT/
        // AVG fold weights into running totals; MIN/MAX walk transitions
        // out of beforeW=0 into the active set). One pass of size
        // |trace|; amortised across subsequent ticks.
        foreach (var (key, group) in Managed.Current)
        {
            object? state = null;
            var agg = _aggregator.Update(ref state, Optional<TOut>.None, group, group);
            _aggCache[key] = agg;
            _stateCache[key] = state;
        }

        if (SnapshotRestoreProfile.Enabled)
        {
            SnapshotRestoreProfile.AddRebuild(SnapshotRestoreProfile.MsSince(tRebuild));
        }

        await MergePersistedStateAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Overlay the persisted per-group state on top of what the fold above produced, then
    /// re-derive each affected group's emitted value from the restored state.
    /// </summary>
    /// <remarks>
    /// <para>Overlay rather than replace: the fold is exact for aggregators whose
    /// accumulator is associative, and a composite aggregate persists only the slots that
    /// need it (see <c>CompositeAggregator.MergePersistedState</c>), so the folded value is
    /// the right answer everywhere a blob is absent.</para>
    /// <para>The emitted value is recovered by calling <c>Update</c> with an <b>empty</b>
    /// delta, which every aggregator answers from its current state without mutating it —
    /// an invariant pinned per aggregate kind by <c>AggregatorEmptyDeltaTests</c>. That
    /// avoids persisting the value separately.</para>
    /// <para>No blobs (an older snapshot, a codec without keyed-blob support, or an
    /// aggregator that needs none) leaves the folded reconstruction exactly as it was.</para>
    /// </remarks>
    private async ValueTask MergePersistedStateAsync(
        ISnapshotReader reader, CancellationToken cancellationToken)
    {
        if (!_aggregator.CanPersistState || !_snapshotCodec!.SupportsKeyedBlobs)
        {
            return;
        }

        var blobs = await _snapshotCodec
            .LoadKeyedBlobsAsync(reader, "aggstate.arrows", cancellationToken)
            .ConfigureAwait(false);

        foreach (var (key, blob) in blobs)
        {
            if (!Managed.Current.ContainsKey(key))
            {
                continue;   // a group the trace no longer holds; its state is moot
            }

            var group = Managed.Current.GroupFor(key);
            _stateCache.TryGetValue(key, out var state);
            _aggregator.MergePersistedState(ref state, blob);
            _stateCache[key] = state;
            _aggCache[key] = _aggregator.Update(ref state, Optional<TOut>.None, ZSet<TValue, Z64>.Empty, group);
            _stateCache[key] = state;
        }
    }

    // The eager restore path only runs over a managed trace (a blocked one is always lazy).
    private ManagedGroupTrace<TKey, TValue, Z64> Managed => (ManagedGroupTrace<TKey, TValue, Z64>)_trace;

    /// <summary>Whether the trace is held as blocks (DBSPNET_TRACE_BLOCKS).</summary>
    internal bool IsBlocked => _trace is Blocks.BlockedGroupTrace;

    public string SchemaFingerprint => _snapshotCodec?.SchemaFingerprint ?? string.Empty;

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            // No input deltas to process, but the frontier may still
            // have advanced — drop sub-frontier state before returning.
            // Mirrors DistinctOp's empty-path CollectGarbage. An idle tick
            // is also when a blocked trace freezes.
            _trace.Idle();
            _output.SetCurrent(ZSet<(TKey, TOut), Z64>.Empty);
            CollectGarbage();
            return;
        }

        var before = _trace;

        ZSetBuilder<(TKey, TOut), Z64> builder;
        if (_pooledBuilder is not null)
        {
            _pooledBuilder.Reset();
            builder = _pooledBuilder;
        }
        else
        {
            builder = new ZSetBuilder<(TKey, TOut), Z64>(_lastOutputSize);
        }

        foreach (var key in delta.Keys)
        {
            var groupDelta = delta.GroupFor(key);
            var beforeGroup = before.GroupFor(key);
            if (_cachesBuilt is not null)
            {
                EnsureCaches(key, beforeGroup);
            }

            // The post-delta group. By default a lazy view over (beforeGroup,
            // groupDelta) that the incremental aggregators probe per delta row —
            // avoiding the eager `beforeGroup + groupDelta` rebuild, which
            // re-hashes the whole group every tick (ZSetBuilder.From; O(K²) over a
            // growing group; docs/design-row-representation.md §14). The eager
            // path is kept behind a seam for the A/B benchmark; both are
            // correctness-equivalent (LazyMergeMultiset.IsEmpty matches the
            // post-cancellation (before+delta).IsEmpty). Passed as the IMultiset
            // parameter of EmitForKey so the concrete type stays at the call site
            // (the interface is unavoidable in IAggregator.Update regardless).
            if (FlatAggregateMode.ForceEagerRebuild)
            {
                EmitForKey(key, groupDelta, beforeGroup + groupDelta, builder);
            }
            else
            {
                EmitForKey(key, groupDelta, new LazyMergeMultiset<TValue>(beforeGroup, groupDelta), builder);
            }
        }

        var result = _pooledBuilder is not null ? builder.BuildShared() : builder.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);
        _trace.Integrate(delta);
        CollectGarbage();
    }

    /// <summary>
    /// Runs the aggregator for one changed group: hand it the cached value, the
    /// per-key delta, and the post-delta multiset, update the caches, and emit a
    /// retract/insert pair if the aggregate changed. Shared by the lazy-view and
    /// eager-rebuild paths (which differ only in the <paramref name="afterGroup"/>
    /// implementation they pass).
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "CA1859:Use concrete types when possible for improved performance",
        Justification = "afterGroup is genuinely polymorphic: the two call sites pass a ZSet (eager) " +
                        "and a LazyMergeMultiset (lazy), so the IMultiset interface is required. " +
                        "IAggregator.Update takes IMultiset regardless, so no dispatch is saved.")]
    private void EmitForKey(
        TKey key,
        ZSet<TValue, Z64> groupDelta,
        IMultiset<TValue, Z64> afterGroup,
        ZSetBuilder<(TKey, TOut), Z64> builder)
    {
        var oldAgg = _aggCache.TryGetValue(key, out var cached)
            ? cached
            : Optional<TOut>.None;
        _stateCache.TryGetValue(key, out var state);

        var newAgg = _aggregator.Update(ref state, oldAgg, groupDelta, afterGroup);

        // Cache-pruning is keyed on "is there literally no trace
        // entry?" (dict-shape IsEmpty), not on the aggregator's
        // linear "group is present" gate. The aggregator's
        // per-key state (e.g. SqlSumAggregator.DistinctNonNullRows)
        // tracks weight transitions across ticks and must persist
        // for cancelling-weight groups where the trace still has
        // entries but the sum is zero. The emission decision
        // (retract vs emit) is handled separately by the
        // oldAgg/newAgg comparison below.
        if (afterGroup.IsEmpty)
        {
            _aggCache.Remove(key);
            _stateCache.Remove(key);
        }
        else
        {
            _aggCache[key] = newAgg;
            _stateCache[key] = state;
        }

        if (oldAgg == newAgg)
        {
            return;
        }

        if (oldAgg.HasValue)
        {
            builder.Add((key, oldAgg.Value), new Z64(-1));
        }

        if (newAgg.HasValue)
        {
            builder.Add((key, newAgg.Value), new Z64(1));
        }
    }

    /// <summary>
    /// After a lazy restore: rebuild <paramref name="key"/>'s caches from its group the first time it
    /// is touched — exactly the eager load's fold for that one group, then its persisted state.
    /// </summary>
    private void EnsureCaches(TKey key, ZSet<TValue, Z64> group)
    {
        if (!_cachesBuilt!.Add(key))
        {
            return;
        }

        byte[]? blob = null;
        _pendingBlobs?.Remove(key, out blob);
        if (group.IsEmpty)
        {
            return;   // a group the trace does not hold: no cache, and its state (if any) is moot
        }

        object? state = null;
        var agg = _aggregator.Update(ref state, Optional<TOut>.None, group, group);
        if (blob is not null)
        {
            _aggregator.MergePersistedState(ref state, blob);
            agg = _aggregator.Update(ref state, Optional<TOut>.None, ZSet<TValue, Z64>.Empty, group);
        }

        _aggCache[key] = agg;
        _stateCache[key] = state;
    }

    /// <summary>
    /// Frontier-driven GC: once the advertised frontier advances, drop every
    /// group whose key is strictly below it — those groups can never be touched
    /// by future input, so their already-emitted aggregate stays in the
    /// downstream view while their trace and cache state is reclaimed. Emits
    /// nothing (GC reduces state, never output). No-op unless both a frontier
    /// and a monotone-key extractor were supplied.
    /// </summary>
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
        var dropped = _trace.DropKeysBelow(frontier, _monotoneKey);
        if (dropped is null)
        {
            // A blocked trace drops lazily and does not list the keys: the caches below the
            // frontier are exactly the groups it held there (a cache is pruned when its group
            // empties), plus restored states not yet applied.
            var monotone = _monotoneKey;
            dropped = [.. _stateCache.Keys.Concat(_aggCache.Keys).Concat(_pendingBlobs?.Keys ?? Enumerable.Empty<TKey>())
                .Distinct().Where(k => monotone(k) < frontier)];
        }

        foreach (var key in dropped)
        {
            _aggCache.Remove(key);
            _stateCache.Remove(key);
            _pendingBlobs?.Remove(key);
            _gcDropped++;
        }
    }

    public string MetricName => "IncrementalAggregate";

    public long RetainedRows => _trace.GroupCount;

    public long LastOutputRows => _output.Current.Count;

    public long? GcFrontier => Metric.Frontier(_frontier);

    public long GcDroppedTotal => _gcDropped;
}
