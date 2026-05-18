using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Persistence;

/// <summary>
/// Default <see cref="ISqlSnapshotCodecs"/> for SQL queries: every codec
/// it produces serialises through Arrow IPC, matching the on-disk
/// conventions used by the rest of <c>DbspNet.Persistence</c>
/// (<c>WalRecorder</c>, <c>Snapshot</c>). Pass an instance to
/// <see cref="PlanToCircuit.Compile"/> to enable
/// <see cref="Snapshot.Write"/> / <see cref="Snapshot.Read"/> on the
/// resulting circuit.
/// </summary>
public sealed class ArrowSqlSnapshotCodecs : ISqlSnapshotCodecs
{
    /// <summary>Shared singleton — every method is stateless.</summary>
    public static ArrowSqlSnapshotCodecs Instance { get; } = new();

    public IZSetTraceCodec<StructuralRow, Z64> CreateZSetTraceCodec(Schema rowSchema) =>
        new ArrowZSetTraceCodec(rowSchema);

    public IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> CreateIndexedZSetTraceCodec(
        Schema keySchema, Schema valueSchema) =>
        new ArrowIndexedZSetTraceCodec(keySchema, valueSchema);
}
