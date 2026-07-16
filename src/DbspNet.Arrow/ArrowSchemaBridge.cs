// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Sql.TypeSystem;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Arrow;

/// <summary>
/// Maps a DbspNet <see cref="SqlSchema"/> to an Apache Arrow
/// <see cref="ArrowSchema"/>. The DbspNet type system has been deliberately
/// aligned to Arrow (see <c>docs/skipped.md</c>): every SQL column type has
/// a same-bit-layout Arrow counterpart, so the mapping here is mechanical
/// and lossless.
/// </summary>
public static class ArrowSchemaBridge
{
    /// <summary>
    /// Convert a DbspNet schema to an Arrow schema, preserving column names
    /// and nullability.
    /// </summary>
    public static ArrowSchema ToArrow(SqlSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var builder = new ArrowSchema.Builder();
        foreach (var col in schema.Columns)
        {
            builder.Field(new Field(col.Name, ToArrowType(col.Type), col.Type.Nullable));
        }

        return builder.Build();
    }

    /// <summary>
    /// Map a single SQL column type to its corresponding Arrow data type.
    /// Layout is identical to Arrow in every case: integer widths match,
    /// strings are UTF-8, decimals are Decimal128 little-endian Int128,
    /// temporals are int days / int microseconds.
    /// </summary>
    public static IArrowType ToArrowType(SqlType type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return type switch
        {
            SqlIntegerType => Int32Type.Default,
            SqlBigintType => Int64Type.Default,
            SqlRealType => FloatType.Default,
            SqlDoubleType => DoubleType.Default,
            SqlDecimalType d => new Decimal128Type(d.Precision, d.Scale),
            SqlVarcharType => StringType.Default,
            SqlBooleanType => BooleanType.Default,
            SqlDateType => Date32Type.Default,
            SqlTimeType => new Time64Type(TimeUnit.Microsecond),
            SqlTimestampType => new TimestampType(TimeUnit.Microsecond, (string?)null),
            _ => throw new NotSupportedException(
                $"no Arrow mapping for SQL type {type.Display}"),
        };
    }

    /// <summary>
    /// Reverse of <see cref="ToArrow"/>: infer a DbspNet <see cref="SqlSchema"/> from
    /// an Arrow schema (a source file/table's schema), preserving column names and
    /// nullability. Used by connectors to register an inferred table schema and to
    /// validate a source against a declared one. Nested / unsupported Arrow types are
    /// rejected in v1 (see <see cref="FromArrowType"/>).
    /// </summary>
    public static SqlSchema FromArrow(ArrowSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var cols = new List<DbspNet.Sql.Plan.SchemaColumn>(schema.FieldsList.Count);
        foreach (var f in schema.FieldsList)
        {
            cols.Add(new DbspNet.Sql.Plan.SchemaColumn(f.Name, FromArrowType(f.DataType, f.IsNullable)));
        }

        return new SqlSchema(cols);
    }

    /// <summary>
    /// Map a single Arrow data type to its DbspNet <see cref="SqlType"/> counterpart.
    /// The inverse of <see cref="ToArrowType"/> over the types DbspNet supports; small
    /// signed integers widen to INTEGER (as <c>TINYINT</c>/<c>SMALLINT</c> already do),
    /// any timestamp/time unit and timezone map to the µs-internal TIMESTAMP/TIME (unit
    /// scaling, if any, is a per-value read concern, not a schema one). Unsigned ints,
    /// 256-bit decimals, and nested/complex types (Struct/List/Map/Union/Binary) are
    /// rejected — a connector must flatten or project them away first (the ROW-flatten
    /// precedent).
    /// </summary>
    public static SqlType FromArrowType(IArrowType type, bool nullable = true)
    {
        ArgumentNullException.ThrowIfNull(type);
        return type switch
        {
            Int8Type or Int16Type or Int32Type => new SqlIntegerType(nullable),
            Int64Type => new SqlBigintType(nullable),
            FloatType => new SqlRealType(nullable),
            DoubleType => new SqlDoubleType(nullable),
            Decimal128Type d => new SqlDecimalType(d.Precision, d.Scale, nullable),
            StringType or LargeStringType => new SqlVarcharType(null, nullable),
            BooleanType => new SqlBooleanType(nullable),
            Date32Type => new SqlDateType(nullable),
            Time64Type => new SqlTimeType(nullable),
            TimestampType => new SqlTimestampType(nullable),
            _ => throw new NotSupportedException(
                $"no SQL mapping for Arrow type '{type.Name}' (TypeId {type.TypeId}); " +
                "nested / unsigned / unsupported types are rejected in v1 — flatten or " +
                "project them away in the connector first"),
        };
    }
}
