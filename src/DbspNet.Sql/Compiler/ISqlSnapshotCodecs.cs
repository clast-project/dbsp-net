using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Factory for the snapshot codecs <see cref="PlanToCircuit"/> needs to
/// inject into stateful operators when compiling a snapshot-enabled
/// circuit. Pass an instance to <see cref="PlanToCircuit.Compile"/> to
/// have operators register a codec at construction; pass <c>null</c>
/// (the default) and operators are built without snapshot support.
/// </summary>
/// <remarks>
/// The interface lives in the SQL layer because the row type
/// (<see cref="StructuralRow"/>) and weight type (<see cref="Z64"/>) are
/// SQL-specific. The implementation typically lives in
/// <c>DbspNet.Persistence</c>, which sees both this layer and
/// <c>DbspNet.Arrow</c>'s IPC machinery.
/// </remarks>
public interface ISqlSnapshotCodecs
{
    /// <summary>
    /// Build a Z-set trace codec for a stream of <see cref="StructuralRow"/>
    /// values whose row layout matches <paramref name="rowSchema"/>.
    /// Used by <c>DistinctOp</c> snapshot integration.
    /// </summary>
    IZSetTraceCodec<StructuralRow, Z64> CreateZSetTraceCodec(Schema rowSchema);

    /// <summary>
    /// Build an indexed Z-set trace codec keyed by GROUP BY columns
    /// (<paramref name="keySchema"/>) holding per-group multisets of
    /// input rows (<paramref name="valueSchema"/>). Used by
    /// <c>IncrementalAggregateOp</c> snapshot integration; the operator
    /// re-bootstraps its per-group aggregator state from the loaded
    /// trace, so the codec only needs to round-trip the trace itself.
    /// </summary>
    IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> CreateIndexedZSetTraceCodec(
        Schema keySchema,
        Schema valueSchema);
}
