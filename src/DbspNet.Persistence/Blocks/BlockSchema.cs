// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow.Ipc;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful.Blocks;
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
internal sealed class BlockSchema : ITraceBlockFormat
{
    private BlockSchema(Schema keySchema, Schema valueSchema, CellKind[] keyKinds, CellKind[] valueKinds)
    {
        KeyKinds = keyKinds;
        ValueKinds = valueKinds;
        Codec = CodecInterning.IndexedCodec(keySchema, valueSchema);
        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, Codec.RunSchema, leaveOpen: true))
        {
            writer.WriteStart();
        }

        SchemaMessage = ms.ToArray();
        SizeEntries();
    }

    /// <summary>The key columns' kinds, in order.</summary>
    public CellKind[] KeyKinds { get; }

    /// <summary>The value columns' kinds, in order.</summary>
    public CellKind[] ValueKinds { get; }

    /// <summary>The run codec whose block encoding this layout reads.</summary>
    internal ArrowIndexedZSetTraceCodec Codec { get; }

    /// <summary>The Arrow IPC schema message (stream start) every block's message follows.</summary>
    internal byte[] SchemaMessage { get; }

    /// <inheritdoc/>
    public int ValueColumns => ValueKinds.Length;

    /// <inheritdoc/>
    public bool Adopts(ITraceBlockFormat other)
    {
        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (other is not BlockSchema theirs
            || !KeyKinds.AsSpan().SequenceEqual(theirs.KeyKinds)
            || !ValueKinds.AsSpan().SequenceEqual(theirs.ValueKinds))
        {
            return false;
        }

        var mine = Codec.RunSchema.FieldsList;
        var those = theirs.Codec.RunSchema.FieldsList;
        if (mine.Count != those.Count)
        {
            return false;
        }

        for (var i = 0; i < mine.Count; i++)
        {
            if (TypeSignature(mine[i].DataType) != TypeSignature(those[i].DataType) || (!mine[i].IsNullable && those[i].IsNullable))
            {
                return false;
            }
        }

        return true;
    }

    // An Arrow type's layout, names aside: precision and scale, time units and zones included.
    private static string TypeSignature(Apache.Arrow.Types.IArrowType type) => type switch
    {
        Apache.Arrow.Types.Decimal128Type d => FormattableString.Invariant($"decimal128({d.Precision},{d.Scale})"),
        Apache.Arrow.Types.TimestampType t => FormattableString.Invariant($"timestamp({t.Unit},{t.Timezone})"),
        Apache.Arrow.Types.Time64Type t => FormattableString.Invariant($"time64({t.Unit})"),
        Apache.Arrow.Types.Time32Type t => FormattableString.Invariant($"time32({t.Unit})"),
        _ => type.TypeId.ToString(),
    };

    /// <inheritdoc/>
    Core.Operators.Stateful.Blocks.IBlockedTraceCodec? ITraceBlockFormat.BackingCodec => Codec;

    /// <inheritdoc/>
    public int EntryBytes(StructuralRow key, StructuralRow value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        var bytes = _fixedBytes;
        foreach (var c in _keyStrings)
        {
            bytes += key[c] is Utf8String s ? s.Span.Length : 0;
        }

        foreach (var c in _valueStrings)
        {
            bytes += value[c] is Utf8String s ? s.Span.Length : 0;
        }

        return bytes;
    }

    int ITraceBlockFormat.GatheredEntryBytes(ITraceBlock source, int index)
    {
        if (source is not TraceBlock block)
        {
            return 0;
        }

        var bytes = _fixedBytes;
        foreach (var c in _keyStrings)
        {
            var offsets = block.ValuesAt(c);
            bytes += System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(block.Bytes.AsSpan(offsets + ((index + 1) * 4)))
                - System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(block.Bytes.AsSpan(offsets + (index * 4)));
        }

        return bytes;
    }

    private int _fixedBytes;
    private int[] _keyStrings = [];
    private int[] _valueStrings = [];

    private void SizeEntries()
    {
        static int Width(CellKind kind) => kind switch
        {
            CellKind.Bool => 1,
            CellKind.Int32 or CellKind.Float32 or CellKind.Date32 or CellKind.Utf8 => 4,
            CellKind.Decimal128 => 16,
            _ => 8,
        };

        _fixedBytes = KeyKinds.Sum(Width) + ValueKinds.Sum(Width) + 8 + 4 + 1;
        _keyStrings = [.. Enumerable.Range(0, KeyKinds.Length).Where(c => KeyKinds[c] == CellKind.Utf8)];
        _valueStrings = [.. Enumerable.Range(0, ValueKinds.Length).Where(c => ValueKinds[c] == CellKind.Utf8)];
    }

    ITraceBlock? ITraceBlockFormat.Gather(ITraceBlock[] sources, int[] indexes, long[] values, long[] weights, int[] hashes, int count)
    {
        if (ValueKinds is not [CellKind.Int64])
        {
            return null;
        }

        var bodies = new TraceBlock[count];
        for (var i = 0; i < count; i++)
        {
            if (sources[i] is not TraceBlock body || !body.Schema.KeyKinds.AsSpan().SequenceEqual(KeyKinds))
            {
                return null;
            }

            bodies[i] = body;
        }

        return BlockWriter.Gather(this, bodies, indexes, values, weights, hashes, count);
    }

    ITraceBlock ITraceBlockFormat.Encode(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count) =>
        Encode(keys, values, weights, hashes, count);

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
        return CodecInterning.Format(keySchema, valueSchema, static (k, v) =>
        {
            var keys = Kinds(k);
            var values = Kinds(v);
            return keys is null || values is null ? null : new BlockSchema(k, v, keys, values);
        });
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
    /// key's entries contiguous — as one block (<see cref="BlockWriter"/>).</summary>
    public TraceBlock Encode(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count) =>
        BlockWriter.Write(this, keys, values, weights, hashes, count);

    /// <summary>The same block through Arrow's builders and stream writer — the reference the
    /// hand-written <see cref="BlockWriter"/> is tested against.</summary>
    internal TraceBlock EncodeViaArrow(StructuralRow[] keys, StructuralRow[] values, long[] weights, int[] hashes, int count)
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
    /// <summary>Adopt a block read straight into place: <paramref name="read"/> fills the message's
    /// <paramref name="length"/> bytes.</summary>
    public TraceBlock Adopt(int length, Action<Memory<byte>> read)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (!LegacyParse)
        {
            // The message alone, its header read directly (§3.6); the Arrow path if it is unusual.
            var message = new byte[length];
            read(message);
            return TraceBlock.TryParseHeader(this, message) ?? Adopt(message);
        }

        var stream = new byte[SchemaMessage.Length + length];
        SchemaMessage.CopyTo(stream, 0);
        read(stream.AsMemory(SchemaMessage.Length, length));
        return TraceBlock.Parse(this, stream, SchemaMessage.Length, stream.Length);
    }

    /// <summary><c>DBSPNET_BLOCK_PARSE=arrow</c>: adopt every block through an Arrow reader (the
    /// path before §3.6; tests compare the two).</summary>
    internal static bool LegacyParse { get; set; } =
        string.Equals(Environment.GetEnvironmentVariable("DBSPNET_BLOCK_PARSE"), "arrow", StringComparison.OrdinalIgnoreCase);

    public TraceBlock Adopt(ReadOnlySpan<byte> message)
    {
        var stream = new byte[SchemaMessage.Length + message.Length];
        SchemaMessage.CopyTo(stream, 0);
        message.CopyTo(stream.AsSpan(SchemaMessage.Length));
        return TraceBlock.Parse(this, stream, SchemaMessage.Length, stream.Length);
    }
}
