using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Constructs and inspects rows of a specific row-representation type. Isolates
/// the SQL compiler from the concrete row layout so that alternative
/// representations — emitted-equality subclasses, pooled byte buffers for AOT —
/// can plug in without touching the operator graph.
/// </summary>
/// <remarks>
/// The <see cref="Schema"/> is optional at every <see cref="BuildRow"/> call.
/// Schema-aware codecs (e.g. an emitted-subclass codec) use it to pick a
/// per-schema constructor when available, and fall back to a generic
/// representation when <c>null</c> (used by helpers that build rows of derived
/// schemas not directly available at the call site).
/// <see cref="StructuralRowCodec"/> ignores the parameter entirely.
/// </remarks>
public interface IRowCodec<TRow>
    where TRow : notnull
{
    /// <summary>
    /// Build a row from a positional column-value sequence. <c>null</c> slots
    /// represent SQL NULL; non-null slots are boxed CLR values whose runtime
    /// type matches the schema column's declared
    /// <see cref="TypeSystem.SqlType"/>.
    /// </summary>
    TRow BuildRow(Schema? schema, ReadOnlySpan<object?> values);
}

/// <summary>
/// The reference row codec: produces the standard object-array-backed
/// <see cref="StructuralRow"/>. Ships alongside the SQL compiler today and is
/// retained indefinitely as a differential-testing baseline for future codecs.
/// </summary>
public sealed class StructuralRowCodec : IRowCodec<StructuralRow>
{
    public static StructuralRowCodec Instance { get; } = new();

    private StructuralRowCodec()
    {
    }

    public StructuralRow BuildRow(Schema? schema, ReadOnlySpan<object?> values)
    {
        return StructuralRow.Of(values);
    }
}

