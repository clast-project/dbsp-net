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
    private long _tickCount;

    internal object SyncRoot { get; } = new();

    /// <summary>Number of times <see cref="Step"/> has completed.</summary>
    public long TickCount => Interlocked.Read(ref _tickCount);

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
    }

    /// <summary>
    /// Operators registered with this circuit, in topological-/registration-
    /// order. Exposed for the persistence layer to enumerate snapshottable
    /// operators; operator positions in this list serve as stable ids
    /// (paired with a plan fingerprint that catches structural drift).
    /// </summary>
    internal IReadOnlyList<IOperator> Operators => _operators;

    /// <summary>
    /// Build a circuit by running <paramref name="configure"/> against a
    /// builder. The builder's methods register inputs, outputs and operators
    /// with this circuit.
    /// </summary>
    public static RootCircuit Build(Action<CircuitBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var circuit = new RootCircuit();
        configure(new CircuitBuilder(circuit));
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

            foreach (var op in _operators)
            {
                op.Step();
            }

            Interlocked.Increment(ref _tickCount);
        }
    }
}
