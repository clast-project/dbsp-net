// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.IO.Hashing;
using System.Text;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Persistence;

/// <summary>
/// Canonical hashing of SQL schemas used by snapshot codecs to expose
/// a <c>SchemaFingerprint</c>. The hash is column-name + column-type
/// only (via <c>SqlType.Display</c>, which already includes nullability
/// and parameterised modifiers like <c>VARCHAR(8)</c> or
/// <c>DECIMAL(10,2)</c>) — independent of row data and stable across
/// runs.
/// </summary>
internal static class SchemaFingerprint
{
    /// <summary>
    /// Hash a single schema. Format: <c>name1:type1,name2:type2,...</c>
    /// then xxh3-64 hex.
    /// </summary>
    public static string Of(SqlSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var sb = new StringBuilder();
        AppendSchema(sb, schema);
        return Hash(sb);
    }

    /// <summary>
    /// Hash a (key, value) schema pair, with explicit separators so a
    /// (k, v) and (kv concatenated, empty) round to different fingerprints.
    /// </summary>
    public static string Of(SqlSchema keySchema, SqlSchema valueSchema)
    {
        ArgumentNullException.ThrowIfNull(keySchema);
        ArgumentNullException.ThrowIfNull(valueSchema);
        var sb = new StringBuilder();
        sb.Append("k=");
        AppendSchema(sb, keySchema);
        sb.Append(";v=");
        AppendSchema(sb, valueSchema);
        return Hash(sb);
    }

    private static void AppendSchema(StringBuilder sb, SqlSchema schema)
    {
        sb.Append('[');
        for (var c = 0; c < schema.Count; c++)
        {
            if (c > 0)
            {
                sb.Append(',');
            }

            var col = schema[c];
            sb.Append(col.Name).Append(':').Append(col.Type.Display);
        }

        sb.Append(']');
    }

    private static string Hash(StringBuilder sb)
    {
        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var hash = XxHash3.HashToUInt64(bytes);
        return hash.ToString("x16", System.Globalization.CultureInfo.InvariantCulture);
    }
}
