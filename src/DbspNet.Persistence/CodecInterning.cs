// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Concurrent;
using System.Text;
using DbspNet.Persistence.Blocks;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Persistence;

/// <summary>
/// One snapshot codec and one block format per row shape, process-wide
/// (docs/design-flat-trace-state.md §3.32). Each stateful operator on each worker used to build its
/// own: a codec holds three Arrow schemas, each with its own field lookups, and a block format holds
/// another codec. At SF=3 W=12 that metadata was most of the references the checkpoint heap had
/// left. Codecs and formats are immutable after construction (a codec's lazily built format is a
/// benign race between equal values) and read only column names and types, so equal shapes can
/// share one. <c>DBSPNET_CODEC_INTERN=off</c> builds one per request, as before.
/// </summary>
internal static class CodecInterning
{
    private static readonly bool Enabled =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_CODEC_INTERN"), "off", StringComparison.OrdinalIgnoreCase);

    private static readonly ConcurrentDictionary<string, ArrowZSetTraceCodec> Rows = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, ArrowIndexedZSetTraceCodec> Indexed = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, BlockSchema?> Formats = new(StringComparer.Ordinal);

    public static ArrowZSetTraceCodec RowCodec(SqlSchema rowSchema) =>
        Enabled ? Rows.GetOrAdd(Shape(rowSchema), static (_, s) => new ArrowZSetTraceCodec(s), rowSchema) : new ArrowZSetTraceCodec(rowSchema);

    public static ArrowIndexedZSetTraceCodec IndexedCodec(SqlSchema keySchema, SqlSchema valueSchema) =>
        Enabled
            ? Indexed.GetOrAdd(Shape(keySchema, valueSchema), static (_, s) => new ArrowIndexedZSetTraceCodec(s.Key, s.Value), (Key: keySchema, Value: valueSchema))
            : new ArrowIndexedZSetTraceCodec(keySchema, valueSchema);

    public static BlockSchema? Format(SqlSchema keySchema, SqlSchema valueSchema, Func<SqlSchema, SqlSchema, BlockSchema?> create) =>
        Enabled
            ? Formats.GetOrAdd(Shape(keySchema, valueSchema), static (_, s) => s.Create(s.Key, s.Value), (Key: keySchema, Value: valueSchema, Create: create))
            : create(keySchema, valueSchema);

    // What a codec or format reads of a schema: each column's name and full type (nullability,
    // precision and the rest are in the type record).
    private static string Shape(SqlSchema schema)
    {
        var sb = new StringBuilder();
        Append(sb, schema);
        return sb.ToString();
    }

    private static string Shape(SqlSchema keySchema, SqlSchema valueSchema)
    {
        var sb = new StringBuilder("k=");
        Append(sb, keySchema);
        sb.Append("\u0002v=");
        Append(sb, valueSchema);
        return sb.ToString();
    }

    private static void Append(StringBuilder sb, SqlSchema schema)
    {
        for (var c = 0; c < schema.Count; c++)
        {
            sb.Append(schema[c].Name).Append('\u0001').Append(schema[c].Type).Append('\u0003');
        }
    }
}
