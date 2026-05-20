// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Sql.TypeSystem;
using SqlSchema = DbspNet.Sql.Plan.Schema;
using SqlSchemaColumn = DbspNet.Sql.Plan.SchemaColumn;

namespace DbspNet.Tests.Arrow;

public class ArrowSchemaBridgeTests
{
    [Fact]
    public void MapsAllSupportedScalarTypes()
    {
        var sqlSchema = new SqlSchema(new[]
        {
            new SqlSchemaColumn("i", new SqlIntegerType(false)),
            new SqlSchemaColumn("l", new SqlBigintType(false)),
            new SqlSchemaColumn("r", new SqlRealType(false)),
            new SqlSchemaColumn("d", new SqlDoubleType(false)),
            new SqlSchemaColumn("dec", new SqlDecimalType(10, 2, false)),
            new SqlSchemaColumn("s", new SqlVarcharType(null, false)),
            new SqlSchemaColumn("b", new SqlBooleanType(false)),
            new SqlSchemaColumn("dt", new SqlDateType(false)),
            new SqlSchemaColumn("tm", new SqlTimeType(false)),
            new SqlSchemaColumn("ts", new SqlTimestampType(false)),
        });

        var arrow = ArrowSchemaBridge.ToArrow(sqlSchema);

        Assert.Equal(10, arrow.FieldsList.Count);
        Assert.IsType<Int32Type>(arrow.FieldsList[0].DataType);
        Assert.IsType<Int64Type>(arrow.FieldsList[1].DataType);
        Assert.IsType<FloatType>(arrow.FieldsList[2].DataType);
        Assert.IsType<DoubleType>(arrow.FieldsList[3].DataType);

        var dec = Assert.IsType<Decimal128Type>(arrow.FieldsList[4].DataType);
        Assert.Equal(10, dec.Precision);
        Assert.Equal(2, dec.Scale);

        Assert.IsType<StringType>(arrow.FieldsList[5].DataType);
        Assert.IsType<BooleanType>(arrow.FieldsList[6].DataType);
        Assert.IsType<Date32Type>(arrow.FieldsList[7].DataType);

        var time = Assert.IsType<Time64Type>(arrow.FieldsList[8].DataType);
        Assert.Equal(TimeUnit.Microsecond, time.Unit);

        var ts = Assert.IsType<TimestampType>(arrow.FieldsList[9].DataType);
        Assert.Equal(TimeUnit.Microsecond, ts.Unit);
        Assert.Null(ts.Timezone);
    }

    [Fact]
    public void PreservesFieldNames()
    {
        var sqlSchema = new SqlSchema(new[]
        {
            new SqlSchemaColumn("amount", new SqlDecimalType(10, 2, false)),
            new SqlSchemaColumn("region", new SqlVarcharType(null, false)),
        });

        var arrow = ArrowSchemaBridge.ToArrow(sqlSchema);

        Assert.Equal("amount", arrow.FieldsList[0].Name);
        Assert.Equal("region", arrow.FieldsList[1].Name);
    }

    [Fact]
    public void PropagatesNullability()
    {
        var sqlSchema = new SqlSchema(new[]
        {
            new SqlSchemaColumn("required", new SqlIntegerType(false)),
            new SqlSchemaColumn("optional", new SqlIntegerType(true)),
        });

        var arrow = ArrowSchemaBridge.ToArrow(sqlSchema);

        Assert.False(arrow.FieldsList[0].IsNullable);
        Assert.True(arrow.FieldsList[1].IsNullable);
    }
}
