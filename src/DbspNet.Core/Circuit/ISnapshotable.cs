namespace DbspNet.Core.Circuit;

/// <summary>
/// Optional snapshot contract for operators that hold state across ticks.
/// The persistence layer enumerates the circuit's operators, picks out
/// those implementing this interface, and orchestrates serialisation /
/// restoration through the supplied <see cref="ISnapshotWriter"/> /
/// <see cref="ISnapshotReader"/> contexts.
/// </summary>
/// <remarks>
/// Operator identity is positional: the snapshot is keyed by the
/// operator's index in <c>RootCircuit.Operators</c>. A plan fingerprint
/// stored alongside the snapshot guards against structural drift —
/// adding, removing, or reordering operators changes the fingerprint and
/// makes old snapshots refuse to load.
/// </remarks>
public interface ISnapshotable
{
    /// <summary>Persist the operator's state through <paramref name="writer"/>.</summary>
    void Save(ISnapshotWriter writer);

    /// <summary>Restore the operator's state from <paramref name="reader"/>.</summary>
    void Load(ISnapshotReader reader);

    /// <summary>
    /// Stable hash of the operator's row/key/value schemas, derived from
    /// its snapshot codec(s). The snapshot manifest aggregates these
    /// across the circuit so a load can detect schema drift that the
    /// operator-type fingerprint alone wouldn't see — e.g. VARCHAR
    /// length changes (Arrow's StringType carries no length), DECIMAL
    /// precision/scale changes, or column reorders that leave operator
    /// generic args unchanged. Operators without snapshot codecs return
    /// the empty string; their <see cref="Save"/> already throws.
    /// </summary>
    string SchemaFingerprint { get; }
}

/// <summary>
/// Output side of a snapshot context. Implementations are typically
/// directory-backed (one subdirectory per operator); operators open
/// named files via <see cref="OpenWrite"/> and write whatever format is
/// natural for their state. A small operator might write a single
/// <c>"state.json"</c>; a trace-backed operator might write
/// <c>"keys.arrow"</c>, <c>"values.arrow"</c>, and <c>"manifest.json"</c>.
/// </summary>
public interface ISnapshotWriter
{
    /// <summary>
    /// Open a stream for writing one named artifact. Caller disposes;
    /// implementations are responsible for atomicity at the directory
    /// level (typically tmp+rename on the directory as a whole).
    /// </summary>
    Stream OpenWrite(string filename);
}

/// <summary>
/// Input side of a snapshot context. Mirror of <see cref="ISnapshotWriter"/>.
/// </summary>
public interface ISnapshotReader
{
    /// <summary>Open a stream for reading one named artifact.</summary>
    Stream OpenRead(string filename);

    /// <summary>True if a file with the given name exists in this snapshot.</summary>
    bool Exists(string filename);
}
