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
}
