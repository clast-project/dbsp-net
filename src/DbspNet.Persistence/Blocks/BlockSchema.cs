// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow.Ipc;
using DbspNet.Core.Collections;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Persistence.Blocks;

/// <summary>How one column's cells are stored in a block, and which CLR type a cell decodes to —
/// exactly the type <see cref="DbspNet.Arrow.ArrowColumns"/> produces for the column's SQL type,
/// so a cell read from a block is <c>Equals</c> to the one the run codec would decode.</summary>
internal enum CellKind : byte
{
    /// <summary>INTEGER: <see cref="int"/>, 4 bytes.</summary>
    Int32,

    /// <summary>BIGINT: <see cref="long"/>, 8 bytes.</summary>
    Int64,

    /// <summary>REAL: <see cref="float"/>, 4 bytes.</summary>
    Float32,

    /// <summary>DOUBLE: <see cref="double"/>, 8 bytes.</summary>
    Float64,

    /// <summary>BOOLEAN: <see cref="bool"/>, one bit.</summary>
    Bool,

    /// <summary>VARCHAR: <see cref="Utf8String"/>, int32 offsets plus a UTF-8 heap.</summary>
    Utf8,

    /// <summary>DATE: <see cref="Date32"/>, int32 days.</summary>
    Date32,

    /// <summary>TIME: <see cref="Time64"/>, int64 microseconds.</summary>
    Time64,

    /// <summary>TIMESTAMP: <see cref="Timestamp"/>, int64 microseconds.</summary>
    Timestamp,

    /// <summary>DECIMAL: <see cref="Clast.DatabaseDecimal.Values.Decimal128"/>, 16-byte mantissa.</summary>
    Decimal128,
}

/// <summary>
/// The layout every block of one indexed trace shares (docs/design-flat-trace-state.md §2.2): the
/// v3 run schema (key columns, value columns, <c>__weight</c>, <c>__hash</c>), each column's
/// <see cref="CellKind"/>, and the encoded schema message, which a block's record-batch message
/// follows in a run file.
/// </summary>
internal sealed class BlockSchema
{
    private BlockSchema(Schema keySchema, Schema valueSchema, CellKind[] keyKinds, CellKind[] valueKinds)
    {
        KeyKinds = keyKinds;
        ValueKinds = valueKinds;
        Codec = new ArrowIndexedZSetTraceCodec(keySchema, valueSchema);
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, Codec.RunSchema, leaveOpen: true))
        {
            writer.WriteStart();
        }

        SchemaMessage = ms.ToArray();
    }

    /// <summary>The key columns' kinds, in order.</summary>
    public CellKind[] KeyKinds { get; }

    /// <summary>The value columns' kinds, in order.</summary>
    public CellKind[] ValueKinds { get; }

    /// <summary>The run codec whose block encoding this layout reads.</summary>
    internal ArrowIndexedZSetTraceCodec Codec { get; }

    /// <summary>The Arrow IPC schema message (stream start) every block's message follows.</summary>
    internal byte[] SchemaMessage { get; }

    /// <summary>Column index of <c>__weight</c>.</summary>
    internal int WeightColumn => KeyKinds.Length + ValueKinds.Length;

    /// <summary>Column index of <c>__hash</c>.</summary>
    internal int HashColumn => WeightColumn + 1;

    /// <summary>The layout for a trace keyed by <paramref name="keySchema"/> holding
    /// <paramref name="valueSchema"/> rows, or <see langword="null"/> when a column's type has no
    /// block encoding (the trace then stays managed).</summary>
    public static BlockSchema? TryCreate(Schema keySchema, Schema valueSchema)
    {
        ArgumentNullException.ThrowIfNull(keySchema);
        ArgumentNullException.ThrowIfNull(valueSchema);
        var keys = Kinds(keySchema);
        var values = Kinds(valueSchema);
        return keys is null || values is null ? null : new BlockSchema(keySchema, valueSchema, keys, values);
    }

    private static CellKind[]? Kinds(Schema schema)
    {
        var kinds = new CellKind[schema.Count];
        for (var c = 0; c < schema.Count; c++)
        {
            CellKind? kind = schema[c].Type switch
            {
                SqlIntegerType => CellKind.Int32,
                SqlBigintType => CellKind.Int64,
                SqlRealType => CellKind.Float32,
                SqlDoubleType => CellKind.Float64,
                SqlBooleanType => CellKind.Bool,
                SqlVarcharType => CellKind.Utf8,
                SqlDateType => CellKind.Date32,
                SqlTimeType => CellKind.Time64,
                SqlTimestampType => CellKind.Timestamp,
                SqlDecimalType => CellKind.Decimal128,
                _ => null,
            };
            if (kind is null)
            {
                return null;
            }

            kinds[c] = kind.Value;
        }

        return kinds;
    }

    /// <summary>Encode entries <c>[0, count)</c> — already in ascending unsigned key-hash order, a
    /// key's entries contiguous — as one block.</summary>
    public TraceBlock Encode(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count)
    {
        using var batch = Codec.EncodeBlock(keys, values, weights, hashes, count);
        using var ms = new MemoryStream();
        long end;
        using (var writer = new ArrowStreamWriter(ms, Codec.RunSchema, leaveOpen: true))
        {
            writer.WriteRecordBatch(batch);
            end = ms.Position;
        }

        return TraceBlock.Parse(this, ms.GetBuffer(), SchemaMessage.Length, checked((int)end));
    }

    /// <summary>Adopt a block's record-batch message as stored in a run file (it follows this
    /// schema's message), without decoding a cell.</summary>
    public TraceBlock Adopt(ReadOnlySpan<byte> message)
    {
        var stream = new byte[SchemaMessage.Length + message.Length];
        SchemaMessage.CopyTo(stream, 0);
        message.CopyTo(stream.AsSpan(SchemaMessage.Length));
        return TraceBlock.Parse(this, stream, SchemaMessage.Length, stream.Length);
    }
}
