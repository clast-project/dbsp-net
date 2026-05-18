namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// A SQL data type. Nullability is a property of the type (matching
/// PostgreSQL / Feldera semantics where every column type is either nullable
/// or explicitly NOT NULL).
/// </summary>
public abstract record SqlType(bool Nullable)
{
    /// <summary>The underlying .NET value type for a non-null instance.</summary>
    public abstract Type ClrType { get; }

    /// <summary>Short printable type name (e.g. "INTEGER", "VARCHAR(10)").</summary>
    public abstract string Name { get; }

    public abstract SqlType WithNullable(bool nullable);

    public string Display => Nullable ? Name : Name + " NOT NULL";
}

public sealed record SqlIntegerType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(int);

    public override string Name => "INTEGER";

    public override SqlType WithNullable(bool nullable) => new SqlIntegerType(nullable);
}

public sealed record SqlBigintType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(long);

    public override string Name => "BIGINT";

    public override SqlType WithNullable(bool nullable) => new SqlBigintType(nullable);
}

public sealed record SqlRealType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(float);

    public override string Name => "REAL";

    public override SqlType WithNullable(bool nullable) => new SqlRealType(nullable);
}

public sealed record SqlDoubleType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(double);

    public override string Name => "DOUBLE PRECISION";

    public override SqlType WithNullable(bool nullable) => new SqlDoubleType(nullable);
}

public sealed record SqlDecimalType(int Precision, int Scale, bool Nullable) : SqlType(Nullable)
{
    // Always Decimal128 regardless of declared precision. Multi-tier
    // selection (Decimal32/64/128/256 by precision) is a future optimisation;
    // 128-bit covers the common SQL Server / Substrait range (precision ≤ 38)
    // with no behavioural difference vs. narrower tiers.
    public override Type ClrType => typeof(Clast.DatabaseDecimal.Values.Decimal128);

    public override string Name => $"DECIMAL({Precision},{Scale})";

    public override SqlType WithNullable(bool nullable) => new SqlDecimalType(Precision, Scale, nullable);
}

public sealed record SqlVarcharType(int? MaxLength, bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(Utf8String);

    public override string Name => MaxLength is null ? "VARCHAR" : $"VARCHAR({MaxLength})";

    public override SqlType WithNullable(bool nullable) => new SqlVarcharType(MaxLength, nullable);
}

public sealed record SqlBooleanType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(bool);

    public override string Name => "BOOLEAN";

    public override SqlType WithNullable(bool nullable) => new SqlBooleanType(nullable);
}

public sealed record SqlDateType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(Date32);

    public override string Name => "DATE";

    public override SqlType WithNullable(bool nullable) => new SqlDateType(nullable);
}

public sealed record SqlTimeType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(Time64);

    public override string Name => "TIME";

    public override SqlType WithNullable(bool nullable) => new SqlTimeType(nullable);
}

public sealed record SqlTimestampType(bool Nullable) : SqlType(Nullable)
{
    public override Type ClrType => typeof(Timestamp);

    public override string Name => "TIMESTAMP";

    public override SqlType WithNullable(bool nullable) => new SqlTimestampType(nullable);
}
