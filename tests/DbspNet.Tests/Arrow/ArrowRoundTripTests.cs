// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Apache.Arrow;
using Apache.Arrow.Types;
using Clast.DatabaseDecimal.Values;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Arrow;

public class ArrowRoundTripTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    // ---- Output side: ToArrowDelta ----

    [Fact]
    public void OutputDelta_PassThroughSelect_RoundTripsAllScalarTypes()
    {
        var q = Compile(
            [
                "CREATE TABLE t (i INT NOT NULL, l BIGINT NOT NULL, " +
                "d DOUBLE PRECISION NOT NULL, dec DECIMAL(10,2) NOT NULL, " +
                "s VARCHAR NOT NULL, b BOOLEAN NOT NULL, " +
                "dt DATE NOT NULL, ts TIMESTAMP NOT NULL)",
            ],
            "SELECT i, l, d, dec, s, b, dt, ts FROM t");

        q.Table("t").Insert(
            42, 100_000_000_000L, 3.14, "12.34", "hello", true,
            Date32.Parse("2026-05-04"), Timestamp.Parse("2026-05-04 12:00:00"));
        q.Step();

        var delta = q.ToArrowDelta();

        Assert.Equal(1, delta.Rows.Length);
        Assert.Equal(new long[] { 1 }, delta.Weights);

        Assert.Equal(42, ((Int32Array)delta.Rows.Column(0)).Values[0]);
        Assert.Equal(100_000_000_000L, ((Int64Array)delta.Rows.Column(1)).Values[0]);
        Assert.Equal(3.14, ((DoubleArray)delta.Rows.Column(2)).Values[0]);
        Assert.Equal("hello", ((StringArray)delta.Rows.Column(4)).GetString(0));
        Assert.True(((BooleanArray)delta.Rows.Column(5)).GetValue(0));
    }

    [Fact]
    public void OutputDelta_NegativeWeightsForRetractions()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();

        // First step's delta has +1 weights for both rows.
        var firstDelta = q.ToArrowDelta();
        Assert.Equal(2, firstDelta.Rows.Length);
        Assert.All(firstDelta.Weights, w => Assert.Equal(1, w));

        // Delete row 1 → next step's delta has weight −1 on row 1.
        q.Table("t").Delete(1);
        q.Step();
        var retractDelta = q.ToArrowDelta();
        Assert.Equal(1, retractDelta.Rows.Length);
        Assert.Equal(-1, retractDelta.Weights[0]);
        Assert.Equal(1, ((Int32Array)retractDelta.Rows.Column(0)).Values[0]);
    }

    [Fact]
    public void OutputDelta_NullValues()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR)"],
            "SELECT id, name FROM t");
        q.Table("t").Insert(1, "alice");
        q.Table("t").Insert(2, (object?)null);
        q.Step();

        var delta = q.ToArrowDelta();
        var nameCol = (StringArray)delta.Rows.Column(1);

        // Locate each row by id — Z-set order isn't ordered.
        var idCol = (Int32Array)delta.Rows.Column(0);
        var aliceIdx = idCol.Values[0] == 1 ? 0 : 1;
        var nullIdx = 1 - aliceIdx;
        Assert.Equal("alice", nameCol.GetString(aliceIdx));
        Assert.True(nameCol.IsNull(nullIdx));
    }

    // ---- Input side: PushArrow ----

    [Fact]
    public void InputBatch_RoundTripsThroughCompiledQuery()
    {
        var q = Compile(
            [
                "CREATE TABLE t (id INT NOT NULL, " +
                "amount DECIMAL(10, 2) NOT NULL, name VARCHAR NOT NULL)",
            ],
            "SELECT id, amount, name FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);

        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1).Append(2).Append(3);

        var amtType = new Decimal128Type(10, 2);
        var amtBuilder = new Decimal128Array.Builder(amtType);
        amtBuilder.Append(1234m).Append(2599m).Append(150m);

        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append("alice").Append("bob").Append("carol");

        var batch = new RecordBatch(arrowSchema, new IArrowArray[]
        {
            idBuilder.Build(),
            amtBuilder.Build(),
            nameBuilder.Build(),
        }, length: 3);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(3, delta.Rows.Length);
        Assert.All(delta.Weights, w => Assert.Equal(1, w));
    }

    [Fact]
    public void InputBatch_MixedWeights_AppliesInsertsAndRetractions()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL)"],
            "SELECT id FROM t");
        q.Table("t").Insert(1);
        q.Table("t").Insert(2);
        q.Step();
        Assert.Equal(2, q.Current.Count);

        // Build a batch that retracts id=1 and inserts id=3 in one Step.
        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1).Append(3);
        var batch = new RecordBatch(arrowSchema, new IArrowArray[]
        {
            idBuilder.Build(),
        }, length: 2);

        long[] weights = { -1, 1 };
        q.Table("t").PushArrow(batch, weights);
        q.Step();

        var delta = q.ToArrowDelta();
        // Delta has the retraction (id=1, w=−1) and the insertion (id=3, w=+1).
        Assert.Equal(2, delta.Rows.Length);
        var ids = ((Int32Array)delta.Rows.Column(0));
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            if (ids.Values[i] == 1)
            {
                Assert.Equal(-1, delta.Weights[i]);
            }
            else
            {
                Assert.Equal(1, delta.Weights[i]);
                Assert.Equal(3, ids.Values[i]);
            }
        }
    }

    [Fact]
    public void RoundTrip_WithMultibyteString()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var sb = new StringArray.Builder();
        sb.Append("café").Append("🎉");
        var batch = new RecordBatch(arrowSchema, new IArrowArray[] { sb.Build() }, 2);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        var col = (StringArray)delta.Rows.Column(0);
        var values = new HashSet<string>();
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            values.Add(col.GetString(i));
        }

        Assert.Contains("café", values);
        Assert.Contains("🎉", values);
    }

    [Fact]
    public void InputBatch_DecodesInt96Timestamp()
    {
        // A Delta table whose log schema says TIMESTAMP but whose Parquet stores the legacy
        // INT96 encoding surfaces (via engineered-wood) as FixedSizeBinary(12). The decode is
        // driven by the declared TIMESTAMP type and must reconstruct the µs instant, with nulls.
        var q = Compile(["CREATE TABLE t (ts TIMESTAMP)"], "SELECT ts FROM t");

        var expected = Timestamp.Parse("2026-05-04 12:00:00");
        var days = Math.DivRem(expected.Microseconds, 86_400_000_000L, out var microsOfDay);
        var int96 = new byte[12];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(int96.AsSpan(0, 8), microsOfDay * 1000L);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(int96.AsSpan(8, 4), (int)(days + 2440588));

        var fsbType = new FixedSizeBinaryType(12);
        // FixedSizeBinaryArray has no Builder — construct via ArrayData: row 0 = the INT96
        // bytes, row 1 = 12 padding bytes marked null by the validity bitmap.
        var values = new ArrowBuffer.Builder<byte>().Append(int96).Append(new byte[12]).Build();
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        var arr = new Apache.Arrow.Arrays.FixedSizeBinaryArray(
            new ArrayData(fsbType, length: 2, nullCount: 1, offset: 0, new[] { validity, values }));

        // The batch's Arrow field type is FixedSizeBinary — exactly what engineered-wood yields
        // for INT96 — even though the table column is TIMESTAMP. PushArrow aligns by index.
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("ts", fsbType, nullable: true)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, length: 2);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(2, delta.Rows.Length);
        var ts = (TimestampArray)delta.Rows.Column(0);
        var nonNull = ts.IsNull(0) ? 1 : 0;
        Assert.False(ts.IsNull(nonNull));
        Assert.True(ts.IsNull(1 - nonNull));
        Assert.Equal(expected.Microseconds, ts.Values[nonNull]);
    }

    [Fact]
    public void InputBatch_DecodesNarrowIntAsInteger()
    {
        // A TINYINT/SMALLINT source column is Arrow Int8/Int16 but binds to SQL INTEGER
        // (FromArrowType widens). Decode must accept the narrow width, not just Int32.
        var q = Compile(["CREATE TABLE t (n INT)"], "SELECT n FROM t");

        var i8 = new Int8Array.Builder();
        i8.Append((sbyte)7).AppendNull();
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("n", Int8Type.Default, nullable: true)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { i8.Build() }, length: 2);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(2, delta.Rows.Length);
        var col = (Int32Array)delta.Rows.Column(0);
        var nonNull = col.IsNull(0) ? 1 : 0;
        Assert.Equal(7, col.Values[nonNull]);
        Assert.True(col.IsNull(1 - nonNull));
    }

    [Fact]
    public void NestedRow_ThreeLevelFieldAccess_ResolvesCorrectLeaf()
    {
        // Reproduces the ivm-bench CustomerMgmt shape: a 2-level customer field and a 3-level
        // account field. Flattened leaf columns, in declaration order: Customer._C_ID (col 0),
        // Customer.Account._CA_ID (col 1). If the 3-level field access mis-resolves, caid is
        // wrong/null while cid is right — exactly the dim_customer-ok / dim_account-empty split.
        var q = Compile(
            ["CREATE TABLE t (Customer ROW(_C_ID BIGINT, Account ROW(_CA_ID BIGINT)))"],
            "SELECT cm.Customer._C_ID AS cid, cm.Customer.Account._CA_ID AS caid FROM t cm");

        q.Table("t").Insert(100L, 200L);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(1, delta.Rows.Length);
        Assert.Equal(100L, ((Int64Array)delta.Rows.Column(0)).Values[0]); // cid (2-level)
        Assert.Equal(200L, ((Int64Array)delta.Rows.Column(1)).Values[0]); // caid (3-level)
    }

    [Fact]
    public void InputBatch_DecodesDecimal64AsDecimal()
    {
        // A DECIMAL column whose Parquet physical is INT64 arrives as Decimal64Array but binds
        // to SQL DECIMAL; the decode must widen the 8-byte mantissa, not cast to Decimal128Array.
        var q = Compile(["CREATE TABLE t (price DECIMAL(10, 2))"], "SELECT price FROM t");

        var mantissa = new byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(mantissa, 6789L); // 67.89 at scale 2
        var values = new ArrowBuffer.Builder<byte>().Append(mantissa).Append(new byte[8]).Build();
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Build();
        var arr = new Decimal64Array(
            new ArrayData(new Decimal64Type(10, 2), length: 2, nullCount: 1, offset: 0, new[] { validity, values }));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("price", new Decimal64Type(10, 2), nullable: true)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, length: 2);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        var col = (Decimal128Array)delta.Rows.Column(0);
        var nonNull = new HashSet<long>();
        var nulls = 0;
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            if (col.IsNull(i))
            {
                nulls++;
                continue;
            }

            nonNull.Add(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(col.GetBytes(i)));
        }

        Assert.Equal(1, nulls);
        Assert.Contains(6789L, nonNull);
    }

    [Fact]
    public void RoundTrip_DecimalPreservesScaleAndValue()
    {
        var q = Compile(
            ["CREATE TABLE t (price DECIMAL(10, 2) NOT NULL)"],
            "SELECT price FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var b = new Decimal128Array.Builder(new Decimal128Type(10, 2));
        // Arrow's Decimal128Array.Builder normalises a System.Decimal by
        // multiplying by 10^scale to get the stored mantissa.
        b.Append(123.45m);  // → mantissa 12345 at scale 2
        b.Append(0.99m);    // → mantissa 99
        var batch = new RecordBatch(arrowSchema, new IArrowArray[] { b.Build() }, 2);

        q.Table("t").PushArrow(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        var col = (Decimal128Array)delta.Rows.Column(0);
        var mantissas = new HashSet<long>();
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            var bytes = col.GetBytes(i);
            mantissas.Add(System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(bytes));
        }

        Assert.Contains(12345L, mantissas);
        Assert.Contains(99L, mantissas);
    }

    [Fact]
    public void RoundTrip_DecimalEdgeValues_BitExact()
    {
        // Exercises the MemoryMarshal-based zero-copy path with values that
        // stress the sign bit, lower/upper Int128 halves, and near-Int128
        // capacity. If endianness or sign extension was off, these would
        // round-trip to the wrong mantissa.
        var q = Compile(
            ["CREATE TABLE t (v DECIMAL(38, 0) NOT NULL)"],
            "SELECT v FROM t");

        // Carefully chosen mantissas:
        //  - 1: smallest positive
        //  - -1: smallest negative (tests sign extension)
        //  - 2^63: requires upper Int64 half
        //  - 99999999999999999999999999999999999999: 38 nines, near Int128 cap
        //  - -99999999999999999999999999999999999999: same magnitude negated
        q.Table("t").Insert("1");
        q.Table("t").Insert("-1");
        q.Table("t").Insert("9223372036854775808");
        q.Table("t").Insert("99999999999999999999999999999999999999");
        q.Table("t").Insert("-99999999999999999999999999999999999999");
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(5, delta.Rows.Length);

        var col = (Decimal128Array)delta.Rows.Column(0);
        var seen = new HashSet<Int128>();
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            var bytes = col.GetBytes(i);
            seen.Add(System.Runtime.InteropServices.MemoryMarshal.Read<Int128>(bytes));
        }

        Assert.Contains((Int128)1, seen);
        Assert.Contains((Int128)(-1), seen);
        Assert.Contains((Int128)1 << 63, seen);

        var bigPos = Int128.Parse(
            "99999999999999999999999999999999999999",
            System.Globalization.CultureInfo.InvariantCulture);
        Assert.Contains(bigPos, seen);
        Assert.Contains(-bigPos, seen);
    }

    [Fact]
    public void RoundTrip_TemporalTypes()
    {
        var q = Compile(
            ["CREATE TABLE t (d DATE NOT NULL, ts TIMESTAMP NOT NULL)"],
            "SELECT d, ts FROM t");

        var d = Date32.Parse("2026-05-04");
        var ts = Timestamp.Parse("2026-05-04 12:30:45");

        q.Table("t").Insert(d, ts);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(1, delta.Rows.Length);

        var dateCol = (Date32Array)delta.Rows.Column(0);
        var tsCol = (TimestampArray)delta.Rows.Column(1);

        Assert.Equal(d.Days, dateCol.Values[0]);
        Assert.Equal(ts.Microseconds, tsCol.Values[0]);
    }
}
