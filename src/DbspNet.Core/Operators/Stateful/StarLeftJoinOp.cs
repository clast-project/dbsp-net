// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>One dimension of a <see cref="StarLeftJoinOp"/>: its right side, indexed by its join key
/// (NULL keys dropped, broadcast at W&gt;1), the trace that integrates it, and how to read this
/// member's key back out of the fact trace's tuple key.</summary>
internal sealed class StarMember(
    Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> rightIn,
    IGroupTrace<StructuralRow, StructuralRow, Z64> rightTrace,
    IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? rightCodec,
    Func<StructuralRow, StructuralRow?> keyOfTuple,
    Func<StructuralRow, StructuralRow, bool>? residual)
{
    public Stream<IndexedZSet<StructuralRow, StructuralRow, Z64>> RightIn { get; } = rightIn;

    public IGroupTrace<StructuralRow, StructuralRow, Z64> RightTrace { get; } = rightTrace;

    public IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? RightCodec { get; } = rightCodec;

    /// <summary>This member's join key, read from the fact's tuple key; null when a component is
    /// NULL (the member then pads: a NULL key matches nothing).</summary>
    public Func<StructuralRow, StructuralRow?> KeyOfTuple { get; } = keyOfTuple;

    /// <summary>A pair residual over (the run's input row, this member's row), or null.</summary>
    public Func<StructuralRow, StructuralRow, bool>? Residual { get; } = residual;
}

/// <summary>
/// A run of LEFT joins of one fact against small dimensions, every join key computed from the run's
/// input row, as one operator (docs/design-bounded-memory.md §5.48): one trace of the fact, keyed by
/// the tuple of the members' keys, and each output row built once from the fact row and one match
/// (or a NULL pad) per member.
/// </summary>
/// <remarks>
/// <para>With <c>g(r, D)</c> the chain's output for one input row (per member, every row of its key
/// that passes its residual, weights multiplied, or one NULL pad when none does), the chain's output
/// is <c>Σ_r w(r)·g(r, D₁…Dₖ)</c>: linear in the fact for fixed dimensions. So a step emits
/// <c>Σ_{r∈ΔF} g(r, D′) + Σ_{r∈F affected} (g(r, D′) − g(r, D))</c>, with <c>D′ = D + ΔD</c> and r
/// affected when some member's key of r is a key ΔDⱼ touches. With no ΔD (the fact's ticks) a step is
/// a streaming map plus one integrate, and never reads the fact trace.</para>
/// <para>Affected rows are found through a directory of the tuples the trace holds, then read by
/// <c>GroupFor</c>. The directory is a superset (a tuple whose rows cancel stays listed) and capped at
/// <see cref="DirectoryCap"/> tuples; past the cap, and after a restore, a dimension change scans the
/// trace's keys instead (and rebuilds the directory if it fits).</para>
/// </remarks>
internal sealed class StarLeftJoinOp : IOperator, ISnapshotable, IIntrospectable
{
    private const string FactFile = "fact.arrows";

    private readonly Stream<ZSet<StructuralRow, Z64>> _factIn;
    private readonly Func<StructuralRow, StructuralRow?>? _pre;
    private readonly Func<StructuralRow, StructuralRow> _tupleOf;
    private readonly StarMember[] _members;
    private readonly Func<StructuralRow, StructuralRow?[], StructuralRow> _build;
    private readonly IGroupTrace<StructuralRow, StructuralRow, Z64> _fact;
    private readonly IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? _factCodec;
    private readonly Stream<ZSet<StructuralRow, Z64>> _output;

    // Per member, the dimension's groups by key as of the last integrate (a per-key cache over its
    // trace: a blocked dimension would decode a group per fact row otherwise). Kept across ticks;
    // a tick that changes the dimension starts a new one.
    private readonly Dictionary<StructuralRow, ZSet<StructuralRow, Z64>>[] _groups;

    // This tick's: the new groups of a changed member (old + delta), and its delta.
    private readonly Dictionary<StructuralRow, ZSet<StructuralRow, Z64>>?[] _newGroups;
    private readonly IndexedZSet<StructuralRow, StructuralRow, Z64>?[] _deltas;

    private readonly StructuralRow?[] _matches;

    // The tuples the fact trace holds (a superset), or null: unknown (overflowed or restored).
    private HashSet<StructuralRow>? _tuples = [];

    // A directory a key scan rebuilt this tick, listed once the tick's delta is in it.
    private HashSet<StructuralRow>? _rebuilt;

    private int _lastOutputSize;

    public StarLeftJoinOp(
        Stream<ZSet<StructuralRow, Z64>> factIn,
        Func<StructuralRow, StructuralRow?>? pre,
        Func<StructuralRow, StructuralRow> tupleOf,
        StarMember[] members,
        Func<StructuralRow, StructuralRow?[], StructuralRow> build,
        IGroupTrace<StructuralRow, StructuralRow, Z64> factTrace,
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? factCodec,
        Stream<ZSet<StructuralRow, Z64>> output)
    {
        _factIn = factIn;
        _pre = pre;
        _tupleOf = tupleOf;
        _members = members;
        _build = build;
        _fact = factTrace;
        _factCodec = factCodec;
        _output = output;
        _groups = new Dictionary<StructuralRow, ZSet<StructuralRow, Z64>>[members.Length];
        for (var j = 0; j < members.Length; j++)
        {
            _groups[j] = [];
        }

        _newGroups = new Dictionary<StructuralRow, ZSet<StructuralRow, Z64>>?[members.Length];
        _deltas = new IndexedZSet<StructuralRow, StructuralRow, Z64>?[members.Length];
        _matches = new StructuralRow?[members.Length];
    }

    // A member's cached groups: a fact key with no dimension row caches an empty group, so the cache
    // follows the fact's key cardinality on that column; past this it starts over.
    private const int GroupCacheCap = 1 << 17;

    /// <summary>The most tuples the directory lists before it gives up (a test seam).</summary>
    internal static int DirectoryCap { get; set; } = 1 << 16;

    /// <summary>Diagnostics (tests): fact rows re-evaluated for a dimension change, and full scans
    /// of the fact trace's keys, since start.</summary>
    internal long AffectedRows { get; private set; }

    internal long KeyScans { get; private set; }

    /// <summary>Whether the directory is current (tests).</summary>
    internal bool HasDirectory => _tuples is not null;

    internal int MemberCount => _members.Length;

    public void Step()
    {
        var df = _factIn.Current;
        var changed = false;
        for (var j = 0; j < _members.Length; j++)
        {
            var d = _members[j].RightIn.Current;
            _deltas[j] = d.IsEmpty ? null : d;
            _newGroups[j] = d.IsEmpty ? null : [];
            changed |= !d.IsEmpty;
        }

        if (df.IsEmpty && !changed)
        {
            _fact.Idle();
            foreach (var m in _members)
            {
                m.RightTrace.Idle();
            }

            _output.SetCurrent(ZSet<StructuralRow, Z64>.Empty);
            return;
        }

        var output = DeltaSpill.For<StructuralRow, Z64>(_output.DeltaFormat, _lastOutputSize);
        // The fact's indexed delta in the trace's own block format, so a huge one spills as it is built
        // and the trace adopts its runs (D4, §5.22), as an index pass's would.
        var index = DeltaSpillGroups.For<StructuralRow, StructuralRow, Z64>((_fact as BlockedGroupTrace)?.Trace.Format);
        foreach (var (row, w) in df)
        {
            if ((_pre is null ? row : _pre(row)) is not { } r)
            {
                continue;
            }

            var tuple = _tupleOf(r);
            index.Add(tuple, r, w);

            // The directory lists every tuple the trace may hold; a ΔF tuple's old rows are read below
            // before the integrate, so listing it now is harmless.
            if (_tuples is { } tuples && tuples.Add(tuple) && tuples.Count > DirectoryCap)
            {
                _tuples = null;
            }

            Expand(output, r, tuple, w.Value, fresh: true, 0, 1);
        }

        if (changed)
        {
            // Rows already in the fact whose matches may move: re-evaluated against the new dimensions,
            // their old output retracted.
            foreach (var tuple in AffectedTuples())
            {
                foreach (var (r, w) in _fact.GroupFor(tuple))
                {
                    AffectedRows++;
                    Expand(output, r, tuple, w.Value, fresh: true, 0, 1);
                    Expand(output, r, tuple, -w.Value, fresh: false, 0, 1);
                }
            }
        }

        var result = output.Build();
        _lastOutputSize = result.Count;
        _output.SetCurrent(result);

        var delta = index.Build();
        if (_rebuilt is { } rebuilt)
        {
            // A directory rebuilt by this tick's key scan, which saw only the old fact: add the delta's
            // tuples (a rare tick; a block-backed delta's keys are decoded here).
            foreach (var (tuple, _) in delta)
            {
                if (rebuilt.Add(tuple) && rebuilt.Count > DirectoryCap)
                {
                    rebuilt = null;
                    break;
                }
            }

            _tuples = rebuilt;
            _rebuilt = null;
        }

        if (delta.IsEmpty)
        {
            _fact.Idle();
        }
        else
        {
            _fact.Integrate(delta);
        }

        for (var j = 0; j < _members.Length; j++)
        {
            var m = _members[j];
            if (_deltas[j] is { } d)
            {
                m.RightTrace.Integrate(d);
                _groups[j] = _newGroups[j]!;
                _newGroups[j] = null;
                _deltas[j] = null;
            }
            else
            {
                m.RightTrace.Idle();
            }
        }
    }

    // The tuples whose rows some member's changed key reaches: from the directory, or by scanning
    // the trace's keys (and rebuilding the directory on the way, if it fits).
    private IEnumerable<StructuralRow> AffectedTuples()
    {
        var touched = new HashSet<StructuralRow>?[_members.Length];
        for (var j = 0; j < _members.Length; j++)
        {
            if (_deltas[j] is { } d)
            {
                touched[j] = [];
                foreach (var (key, _) in d)
                {
                    touched[j]!.Add(key);
                }
            }
        }

        bool Affected(StructuralRow tuple)
        {
            for (var j = 0; j < _members.Length; j++)
            {
                if (touched[j] is { } keys && _members[j].KeyOfTuple(tuple) is { } k && keys.Contains(k))
                {
                    return true;
                }
            }

            return false;
        }

        var affected = new List<StructuralRow>();
        if (_tuples is { } tuples)
        {
            foreach (var tuple in tuples)
            {
                if (Affected(tuple))
                {
                    affected.Add(tuple);
                }
            }

            return affected;
        }

        KeyScans++;
        var rebuilt = new HashSet<StructuralRow>();
        foreach (var tuple in FactKeys())
        {
            if (rebuilt is not null && rebuilt.Add(tuple) && rebuilt.Count > DirectoryCap)
            {
                rebuilt = null;
            }

            if (Affected(tuple))
            {
                affected.Add(tuple);
            }
        }

        // Listed after this tick's delta is added (Step), or the directory would miss its tuples.
        _rebuilt = rebuilt;

        // A key the scan saw twice (it spans runs) is listed once.
        return affected.Distinct();
    }

    // Every key of the fact trace (a key may repeat).
    private IEnumerable<StructuralRow> FactKeys()
    {
        switch (_fact)
        {
            case ManagedGroupTrace<StructuralRow, StructuralRow, Z64> managed:
                foreach (var (key, _) in managed.Current)
                {
                    yield return key;
                }

                yield break;
            case BlockedGroupTrace blocked:
                if (blocked.Trace.StreamEntries() is { } entries)
                {
                    foreach (var (block, i, _) in entries)
                    {
                        yield return block.Key(i);
                    }
                }
                else
                {
                    foreach (var (key, _) in blocked.Trace.MaterializeUncached())
                    {
                        yield return key;
                    }
                }

                yield break;
            default:
                throw new NotSupportedException($"star join over a {_fact.GetType().Name}");
        }
    }

    // A member's group for a key: the new dimension (fresh) or the old one.
    private ZSet<StructuralRow, Z64> Group(int j, StructuralRow key, bool fresh)
    {
        var groups = _groups[j];
        if (!groups.TryGetValue(key, out var old))
        {
            old = _members[j].RightTrace.GroupFor(key);
            if (groups.Count >= GroupCacheCap)
            {
                groups.Clear();
            }

            groups[key] = old;
        }

        if (!fresh || _newGroups[j] is not { } newGroups)
        {
            return old;
        }

        if (!newGroups.TryGetValue(key, out var now))
        {
            now = old + _deltas[j]!.GroupFor(key);
            if (newGroups.Count >= GroupCacheCap)
            {
                newGroups.Clear();
            }

            newGroups[key] = now;
        }

        return now;
    }

    // g(r, D) times w into the output: member j's matches (or its pad), then the rest.
    private void Expand(ZSetBuilder<StructuralRow, Z64> output, StructuralRow r, StructuralRow tuple, long w, bool fresh, int j, long weight)
    {
        if (j == _members.Length)
        {
            output.Add(_build(r, _matches), new Z64(w * weight));
            return;
        }

        var m = _members[j];
        var matched = false;
        if (m.KeyOfTuple(tuple) is { } key)
        {
            foreach (var (d, dw) in Group(j, key, fresh))
            {
                if (m.Residual is { } residual && !residual(r, d))
                {
                    continue;
                }

                matched = true;
                _matches[j] = d;
                Expand(output, r, tuple, w, fresh, j + 1, weight * dw.Value);
            }
        }

        if (!matched)
        {
            _matches[j] = null;
            Expand(output, r, tuple, w, fresh, j + 1, weight);
        }
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        await _fact.SaveAsync(Codec(_factCodec), writer, FactFile, cancellationToken).ConfigureAwait(false);
        for (var j = 0; j < _members.Length; j++)
        {
            await _members[j].RightTrace.SaveAsync(Codec(_members[j].RightCodec), writer, DimFile(j), cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        await _fact.LoadAsync(Codec(_factCodec), reader, FactFile, cancellationToken).ConfigureAwait(false);
        for (var j = 0; j < _members.Length; j++)
        {
            await _members[j].RightTrace.LoadAsync(Codec(_members[j].RightCodec), reader, DimFile(j), cancellationToken).ConfigureAwait(false);
            _groups[j].Clear();
        }

        // The directory is not persisted: the first dimension change after a restore scans the keys.
        _tuples = null;
    }

    /// <summary>The snapshot file of member <paramref name="j"/>'s dimension (bottom member first).</summary>
    internal static string DimFile(int j) => $"dim{j}.arrows";

    internal const string FactFileName = FactFile;

    private static IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> Codec(
        IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64>? codec) =>
        codec ?? throw new NotSupportedException(
            "StarLeftJoinOp was constructed without snapshot codecs; compile with snapshot codecs to checkpoint it.");

    public string SchemaFingerprint =>
        $"fact={_factCodec?.SchemaFingerprint ?? ""};" + string.Join(";", _members.Select((m, j) => $"dim{j}={m.RightCodec?.SchemaFingerprint ?? ""}"));

    public string MetricName => "StarLeftJoin";

    public long RetainedRows => _fact.GroupCount + _members.Sum(m => (long)m.RightTrace.GroupCount);

    public long LastOutputRows => _output.LastCount;

    public long? GcFrontier => null;

    public long GcDroppedTotal => 0;
}
