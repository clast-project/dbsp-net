// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// Codecs and block formats are shared per row shape (docs/design-flat-trace-state.md §3.32):
/// equal shapes get one instance whatever their qualifiers or lateness; any difference in a column's
/// name, type, nullability or precision gets another; an unencodable shape stays without a format;
/// and a shared format encodes and decodes correctly from many threads at once.
/// </summary>
public class CodecInterningTests
{
    private static Schema S(params SchemaColumn[] columns) => new(columns);

    [Fact]
    public void EqualShapes_ShareOneInstance_DifferentShapesDoNot()
    {
        var a = S(new SchemaColumn("k", new SqlIntegerType(false), "t"), new SchemaColumn("s", new SqlVarcharType(null, true)));
        var sameShape = S(new SchemaColumn("k", new SqlIntegerType(false), "u", Lateness: 5), new SchemaColumn("s", new SqlVarcharType(null, true), "u"));
        var nullable = S(new SchemaColumn("k", new SqlIntegerType(true)), new SchemaColumn("s", new SqlVarcharType(null, true)));
        var renamed = S(new SchemaColumn("k2", new SqlIntegerType(false)), new SchemaColumn("s", new SqlVarcharType(null, true)));
        var codecs = ArrowSqlSnapshotCodecs.Instance;

        Assert.Same(codecs.CreateZSetTraceCodec(a), codecs.CreateZSetTraceCodec(sameShape));
        Assert.NotSame(codecs.CreateZSetTraceCodec(a), codecs.CreateZSetTraceCodec(nullable));
        Assert.NotSame(codecs.CreateZSetTraceCodec(a), codecs.CreateZSetTraceCodec(renamed));
        Assert.Same(codecs.CreateIndexedZSetTraceCodec(a, Schema.Empty), codecs.CreateIndexedZSetTraceCodec(sameShape, Schema.Empty));
        // (k, v) and (k ++ v, empty) are different shapes.
        var k = S(new SchemaColumn("k", new SqlIntegerType(false)));
        var v = S(new SchemaColumn("s", new SqlVarcharType(null, true)));
        Assert.NotSame(codecs.CreateIndexedZSetTraceCodec(k, v), codecs.CreateIndexedZSetTraceCodec(a, Schema.Empty));

        var d1 = S(new SchemaColumn("d", new SqlDecimalType(10, 2, true)));
        var d2 = S(new SchemaColumn("d", new SqlDecimalType(12, 2, true)));
        Assert.NotSame(codecs.CreateZSetTraceCodec(d1), codecs.CreateZSetTraceCodec(d2));

        Assert.Same(BlockSchema.TryCreate(a, Schema.Empty), BlockSchema.TryCreate(sameShape, Schema.Empty));
        Assert.NotSame(BlockSchema.TryCreate(a, Schema.Empty), BlockSchema.TryCreate(nullable, Schema.Empty));
        var interval = S(new SchemaColumn("i", new SqlIntervalType(IntervalQualifier.Year, true)));
        Assert.Null(BlockSchema.TryCreate(interval, Schema.Empty));
        Assert.Null(BlockSchema.TryCreate(interval, Schema.Empty));
    }

    [Fact]
    public void ASharedFormat_EncodesAndDecodes_FromManyThreadsAtOnce()
    {
        var schema = S(
            new SchemaColumn("id", new SqlIntegerType(false)),
            new SchemaColumn("s", new SqlVarcharType(null, true)),
            new SchemaColumn("x", new SqlDoubleType(true)));
        var format = BlockSchema.TryCreate(Schema.Empty, schema)!;
        var key = new StructuralRow([]);
        Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 12 }, t =>
        {
            for (var round = 0; round < 20; round++)
            {
                var n = 50 + ((t * 7 + round) % 200);
                var keys = new StructuralRow[n];
                var values = new StructuralRow[n];
                var weights = new long[n];
                var hashes = new int[n];
                for (var i = 0; i < n; i++)
                {
                    keys[i] = key;
                    values[i] = new StructuralRow(t * 100_000 + i, i % 5 == 0 ? null : Utf8String.Of("r" + t + "-" + i), i % 7 == 0 ? null : i / 3.0);
                    weights[i] = 1 + (i % 3);
                }

                var block = ((DbspNet.Core.Operators.Stateful.Blocks.ITraceBlockFormat)format).Encode(keys, values, weights, hashes, n);
                Assert.Equal(n, block.Count);
                for (var i = 0; i < n; i++)
                {
                    Assert.Equal(values[i], block.Value(i));
                    Assert.Equal(weights[i], block.Weight(i));
                }
            }
        });
    }
}
