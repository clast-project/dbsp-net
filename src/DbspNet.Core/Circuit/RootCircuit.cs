// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Circuit;

/// <summary>
/// A complete DBSP circuit. Construct via <see cref="Build"/>, feed input
/// via <see cref="InputHandle{T}"/>s, advance time one tick at a time via
/// <see cref="Step"/>, read results via <see cref="OutputHandle{T}"/>s.
/// </summary>
/// <remarks>
/// v1 supports only the <em>root</em> (non-nested, non-feedback) circuit.
/// Nested/child circuits for recursive queries are deferred — see
/// <c>docs/skipped.md</c>.
///
/// A circuit is single-threaded per <see cref="Step"/>. External threads may
/// <see cref="InputHandle{T}.Push"/> at any time; pushes are serialized with
/// step execution by a single lock at the root (<see cref="SyncRoot"/>).
/// </remarks>
public sealed class RootCircuit
{
    private readonly List<IInputCommit> _inputs = [];
    private readonly List<IOperator> _operators = [];
    private readonly List<string?> _operatorLabels = [];

    /// <summary>
    /// Label applied to every operator registered while it is set — the compiler
    /// sets it to the SQL view being compiled so a profile can attribute each
    /// operator to its view. <see langword="null"/> = unlabeled.
    /// </summary>
    internal string? CurrentBuildLabel { get; set; }
    private Dictionary<string, object>? _namedPorts;
    private long _tickCount;
    private long _logicalTime = long.MinValue;
    private long _lastStepTicks;
    private long[]? _opCumTicks;

    /// <summary>
    /// When set, <see cref="Step"/> times each operator individually and
    /// accumulates per-operator wall-clock into an internal buffer, readable via
    /// <see cref="CollectOperatorProfile"/>. Opt-in localization aid (the extra
    /// <c>GetTimestamp</c> per operator per tick perturbs the measured step, so
    /// leave it off for headline timing). Set before the first profiled
    /// <see cref="Step"/>.
    /// </summary>
    public bool ProfileOperators { get; set; }

    internal object SyncRoot { get; } = new();

    /// <summary>Number of times <see cref="Step"/> has completed.</summary>
    public long TickCount => Interlocked.Read(ref _tickCount);

    /// <summary>
    /// The current logical time — the value <c>NOW()</c> resolves to inside
    /// temporal filters — in microseconds since the Unix epoch, the same unit
    /// as a <c>TIMESTAMP</c> value. The host advances it via
    /// <see cref="AdvanceTime"/> before a <see cref="Step"/>; it is never read
    /// from the wall clock, so replay is deterministic.
    /// <see cref="long.MinValue"/> means "not yet set" (logical −∞): a temporal
    /// filter admits nothing until the host has advanced the clock at least once.
    /// </summary>
    public long LogicalTime => Interlocked.Read(ref _logicalTime);

    /// <summary>
    /// The logical clock exposed as a read-only <see cref="IFrontier"/>, for
    /// temporal-filter operators that advance with it (and, in time, for
    /// unification with LATENESS frontiers — the clock <em>is</em> a watermark).
    /// Its <see cref="IFrontier.Value"/> tracks <see cref="LogicalTime"/> live.
    /// </summary>
    public IFrontier Clock => _clock ??= new ClockFrontier(this);

    private ClockFrontier? _clock;

    private sealed class ClockFrontier(RootCircuit owner) : IFrontier
    {
        public long Value => owner.LogicalTime;
    }

    /// <summary>
    /// Advance the logical clock to <paramref name="microsSinceEpoch"/>
    /// (microseconds since the Unix epoch). Call before <see cref="Step"/> so
    /// the tick's temporal filters see the new "now". The clock is a watermark:
    /// it must be monotone non-decreasing — a backward move would make
    /// already-emitted retractions unsound — so a value below the current one
    /// throws. Re-setting the same value is allowed (a tick where no logical
    /// time passed).
    /// </summary>
    public void AdvanceTime(long microsSinceEpoch)
    {
        lock (SyncRoot)
        {
            var current = _logicalTime;
            if (current != long.MinValue && microsSinceEpoch < current)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(microsSinceEpoch),
                    microsSinceEpoch,
                    $"logical clock must be monotone non-decreasing; current is {current}");
            }

            Interlocked.Exchange(ref _logicalTime, microsSinceEpoch);
        }
    }

    /// <summary>
    /// Restore the logical clock to an absolute value when loading a snapshot.
    /// Mirrors <see cref="RestoreTickCount"/>: a snapshot represents end-of-tick
    /// T, so the clock jumps to the value it held then rather than restarting at
    /// "unset". Not for general use — callers must guarantee operator state is
    /// consistent with the supplied time.
    /// </summary>
    internal void RestoreLogicalTime(long value)
    {
        lock (SyncRoot)
        {
            Interlocked.Exchange(ref _logicalTime, value);
        }
    }

    /// <summary>
    /// Restore the tick counter to an absolute value. Used by the
    /// persistence layer when loading a snapshot — the snapshot
    /// represents end-of-tick T, so the circuit's tick counter should
    /// jump to T rather than restart from zero. Not for general use:
    /// callers must guarantee the circuit's operator state is consistent
    /// with the supplied tick count.
    /// </summary>
    internal void RestoreTickCount(long value)
    {
        lock (SyncRoot)
        {
            Interlocked.Exchange(ref _tickCount, value);
        }
    }

    internal void AddInput(IInputCommit input)
    {
        _inputs.Add(input);
    }

    internal void AddOperator(IOperator op)
    {
        _operators.Add(op);
        _operatorLabels.Add(CurrentBuildLabel);
    }

    /// <summary>
    /// Register an input or output handle under a stable, build-independent
    /// <paramref name="name"/>. The name is a logical identity for a circuit
    /// port: because <see cref="Build"/> runs the same constructor closure for
    /// every replica, the same name resolves to "the same logical port" on
    /// every replica — which is how <see cref="ParallelCircuit"/> reaches each
    /// worker's copy of an input/output without relying on closure capture.
    /// Names are optional and unused by a plain single circuit; duplicates
    /// within one circuit are a build error.
    /// </summary>
    internal void RegisterNamedPort(string name, object port)
    {
        _namedPorts ??= [];
        if (!_namedPorts.TryAdd(name, port))
        {
            throw new ArgumentException(
                $"A circuit port named '{name}' is already registered.", nameof(name));
        }
    }

    /// <summary>
    /// Look up a port previously registered via <see cref="RegisterNamedPort"/>,
    /// or <see langword="null"/> if no port carries that name.
    /// </summary>
    internal object? FindNamedPort(string name) =>
        _namedPorts is not null && _namedPorts.TryGetValue(name, out var port) ? port : null;

    /// <summary>
    /// Operators registered with this circuit, in topological-/registration-
    /// order. Exposed for the persistence layer to enumerate snapshottable
    /// operators; operator positions in this list serve as stable ids
    /// (paired with a plan fingerprint that catches structural drift).
    /// </summary>
    internal IReadOnlyList<IOperator> Operators => _operators;

    /// <summary>
    /// Wall-clock duration of the most recent <see cref="Step"/> (the operator
    /// firing loop only, not input commit), for per-tick throughput tracking.
    /// <see cref="TimeSpan.Zero"/> before the first step.
    /// </summary>
    public TimeSpan LastStepDuration =>
        new(Interlocked.Read(ref _lastStepTicks) * TimeSpan.TicksPerSecond / System.Diagnostics.Stopwatch.Frequency);

    /// <summary>
    /// Raw <see cref="System.Diagnostics.Stopwatch"/> ticks of the most recent
    /// <see cref="Step"/>, before the lossy conversion to <see cref="TimeSpan"/>.
    /// Used by the per-worker <see cref="StepProfiler"/> so its phase totals and
    /// the step total share one tick scale.
    /// </summary>
    internal long LastStepRawTicks => Interlocked.Read(ref _lastStepTicks);

    /// <summary>
    /// Snapshot per-operator runtime metrics (state size, last-tick output size,
    /// GC frontier and cumulative GC drops) for every stateful operator, in
    /// registration order. Opt-in observability: call it whenever you want a
    /// reading — e.g. after a <see cref="Step"/> to watch trace state shrink as a
    /// LATENESS / clock frontier advances. Stateless operators are omitted. The
    /// read is O(operators) plus each operator's own cost (O(1) for flat traces;
    /// O(state) for a spine trace, which materialises to count).
    /// </summary>
    public IReadOnlyList<OperatorStat> CollectStats()
    {
        var stats = new List<OperatorStat>();
        for (var i = 0; i < _operators.Count; i++)
        {
            if (_operators[i] is IIntrospectable m)
            {
                stats.Add(new OperatorStat(
                    i, m.MetricName, m.RetainedRows, m.LastOutputRows, m.GcFrontier, m.GcDroppedTotal));
            }
        }

        return stats;
    }

    /// <summary>
    /// Build a circuit by running <paramref name="configure"/> against a
    /// builder. The builder's methods register inputs, outputs and operators
    /// with this circuit.
    /// </summary>
    public static RootCircuit Build(Action<CircuitBuilder> configure) => Build(configure, parallel: null);

    /// <summary>
    /// Build a circuit replica for a <see cref="ParallelCircuit"/>, threading the
    /// per-replica <paramref name="parallel"/> context so the builder can wire
    /// this replica's <c>exchange</c> operators to their worker identity and
    /// shared coordinators. With a <see langword="null"/> context (the public
    /// overload) exchanges degrade to the identity.
    /// </summary>
    internal static RootCircuit Build(Action<CircuitBuilder> configure, ParallelBuildContext? parallel)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var circuit = new RootCircuit();
        configure(new CircuitBuilder(circuit, parallel));
        return circuit;
    }

    /// <summary>
    /// Advance the circuit by one logical tick. The tick atomically:
    /// (1) commits all queued input pushes,
    /// (2) fires every operator in topological order,
    /// (3) exposes the outputs via their <see cref="OutputHandle{T}"/>s.
    /// </summary>
    public void Step()
    {
        lock (SyncRoot)
        {
            foreach (var input in _inputs)
            {
                input.Commit();
            }

            var start = System.Diagnostics.Stopwatch.GetTimestamp();
            if (ProfileOperators)
            {
                StepProfiled();
            }
            else
            {
                foreach (var op in _operators)
                {
                    op.Step();
                }
            }

            Interlocked.Exchange(ref _lastStepTicks, System.Diagnostics.Stopwatch.GetTimestamp() - start);
            Interlocked.Increment(ref _tickCount);
        }
    }

    // Instrumented firing loop — one GetTimestamp per operator, accumulated into
    // _opCumTicks by registration index. Only reached when ProfileOperators is set.
    private void StepProfiled()
    {
        _opCumTicks ??= new long[_operators.Count];
        for (var i = 0; i < _operators.Count; i++)
        {
            var t0 = System.Diagnostics.Stopwatch.GetTimestamp();
            _operators[i].Step();
            _opCumTicks[i] += System.Diagnostics.Stopwatch.GetTimestamp() - t0;
        }
    }

    /// <summary>
    /// Zero the per-operator cumulative timers, so the next run of profiled
    /// <see cref="Step"/>s starts fresh. Call at the start of a batch to get a
    /// per-batch (rather than since-creation) operator profile.
    /// </summary>
    public void ResetOperatorProfile()
    {
        if (_opCumTicks is not null)
        {
            System.Array.Clear(_opCumTicks);
        }
    }

    /// <summary>
    /// Per-operator cumulative wall-clock across every profiled <see cref="Step"/>
    /// so far, one entry per registered operator, in registration order. Requires
    /// <see cref="ProfileOperators"/> to have been set before stepping; returns an
    /// empty list otherwise. Pairs each operator's total time with its type label
    /// and (for stateful operators) its retained- and last-output-row counts, so a
    /// caller can rank the circuit's hot operators after a batch.
    /// </summary>
    public IReadOnlyList<OperatorProfile> CollectOperatorProfile()
    {
        if (_opCumTicks is null)
        {
            return System.Array.Empty<OperatorProfile>();
        }

        var freq = System.Diagnostics.Stopwatch.Frequency;
        var profiles = new List<OperatorProfile>(_operators.Count);
        for (var i = 0; i < _operators.Count; i++)
        {
            var op = _operators[i];
            var ms = _opCumTicks[i] * 1000.0 / freq;
            var label = i < _operatorLabels.Count ? _operatorLabels[i] : null;
            if (op is IIntrospectable m)
            {
                profiles.Add(new OperatorProfile(i, m.MetricName, ms, m.RetainedRows, m.LastOutputRows, label));
            }
            else
            {
                var name = op.GetType().Name;
                var tick = name.IndexOf('`');
                if (tick >= 0)
                {
                    name = name[..tick];
                }

                profiles.Add(new OperatorProfile(i, name, ms, -1, -1, label));
            }
        }

        return profiles;
    }
}
