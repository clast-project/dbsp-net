// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.InteropServices;
using Apache.Arrow.Ipc;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Collections;
using DbspNet.Persistence.Blocks;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Xunit;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// The block reader of docs/design-flat-trace-state.md §2.2/§2.4, held against the production run
/// decoder (<c>ArrowIndexedZSetTraceCodec.DecodeEntries</c>) over random schemas of every engine
/// type: cells decode to the same values and CLR types, and encoded-key equality agrees with
/// cell-wise <c>object.Equals</c> — NULLs, ±0.0, NaN payloads, int vs long, empty and non-ASCII
/// strings included.
/// </summary>
public class TraceBlockTests
{
    private static readonly CellKind[] AllKinds = Enum.GetValues<CellKind>();

    private static SqlType TypeOf(CellKind kind, bool nullable) => kind switch
    {
        CellKind.Int32 => new SqlIntegerType(nullable),
        CellKind.Int64 => new SqlBigintType(nullable),
        CellKind.Float32 => new SqlRealType(nullable),
        CellKind.Float64 => new SqlDoubleType(nullable),
        CellKind.Bool => new SqlBooleanType(nullable),
        CellKind.Utf8 => new SqlVarcharType(null, nullable),
        CellKind.Date32 => new SqlDateType(nullable),
        CellKind.Time64 => new SqlTimeType(nullable),
        CellKind.Timestamp => new SqlTimestampType(nullable),
        _ => new SqlDecimalType(38, 2, nullable),
    };

    private static readonly float[] Floats = [0f, -0f, float.NaN, BitConverter.Int32BitsToSingle(0x7fc00001), float.PositiveInfinity, float.NegativeInfinity, 1.5f, float.Epsilon];

    private static readonly double[] Doubles =
        [0d, -0d, double.NaN, BitConverter.Int64BitsToDouble(0x7ff8000000000001), double.PositiveInfinity, double.NegativeInfinity, 1.5, double.Epsilon];

    private static readonly string[] Strings = ["", "a", "ab", "é", "日本語", "\U0001F600", "a\0b", "long-ish string value"];

    // A small pool per kind, so random rows collide and probes hit.
    private static object Cell(CellKind kind, Random rnd) => kind switch
    {
        CellKind.Int32 => rnd.Next(4) switch { 0 => int.MinValue, 1 => int.MaxValue, _ => rnd.Next(-3, 4) },
        CellKind.Int64 => rnd.Next(4) switch { 0 => long.MinValue, 1 => long.MaxValue, _ => (long)rnd.Next(-3, 4) },
        CellKind.Float32 => Floats[rnd.Next(Floats.Length)],
        CellKind.Float64 => Doubles[rnd.Next(Doubles.Length)],
        CellKind.Bool => rnd.Next(2) == 0,
        CellKind.Utf8 => Utf8String.Of(Strings[rnd.Next(Strings.Length)]),
        CellKind.Date32 => new Date32(rnd.Next(4) switch { 0 => -719_162, 1 => 2_932_896, _ => rnd.Next(-3, 4) }),
        CellKind.Time64 => new Time64(rnd.Next(3) == 0 ? 86_399_999_999L : rnd.Next(0, 4)),
        CellKind.Timestamp => new Timestamp(rnd.Next(4) switch { 0 => -62_135_596_800_000_000L, 1 => 253_402_300_799_999_999L, _ => rnd.Next(-3, 4) }),
        _ => new Decimal128(rnd.Next(3) switch { 0 => Int128.Parse("99999999999999999999999999999999999999", System.Globalization.CultureInfo.InvariantCulture), 1 => -(Int128)long.MaxValue * 7, _ => rnd.Next(-3, 4) }),
    };

    // The same value under another CLR type, or a near miss, or a NULL: what a wrong probe looks like.
    private static object? Mutate(object? cell, Random rnd) => rnd.Next(3) == 0 ? null : cell switch
    {
        null => 0,
        int i => rnd.Next(2) == 0 ? (long)i : i + 1,
        long l => rnd.Next(2) == 0 ? (int)l : l - 1,
        float f => rnd.Next(2) == 0 ? (double)f : (f == 0f ? (BitConverter.SingleToInt32Bits(f) == 0 ? -0f : 0f) : -f),
        double d => rnd.Next(2) == 0 ? (float)d : (d == 0d ? (BitConverter.DoubleToInt64Bits(d) == 0 ? -0d : 0d) : double.IsNaN(d) ? double.NaN : -d),
        bool b => rnd.Next(2) == 0 ? (object)(b ? 1 : 0) : !b,
        Utf8String u => rnd.Next(2) == 0 ? u.ToString() : Utf8String.Of(u.ToString() + "x"),
        Date32 d => rnd.Next(2) == 0 ? d.Days : new Date32(d.Days + 1),
        Time64 t => new Timestamp(t.Microseconds),
        Timestamp t => new Time64(t.Microseconds),
        Decimal128 m => rnd.Next(2) == 0 ? (object)(long)m.Mantissa : new Decimal128(m.Mantissa + 1),
        _ => cell,
    };

    // Build the message only on failure: cells near the type limits do not all print.
    private static void Check(bool condition, Func<string> message)
    {
        if (!condition)
        {
            Assert.Fail(Safe(message));
        }
    }

    private static string Safe(Func<string> message)
    {
        try
        {
            return message();
        }
        catch (ArgumentOutOfRangeException)
        {
            return "(a cell near its type's limit does not print)";
        }
    }

    private static bool CellwiseEquals(StructuralRow a, StructuralRow b)
    {
        if (a.Count != b.Count)
        {
            return false;
        }

        for (var c = 0; c < a.Count; c++)
        {
            if (!Equals(a[c], b[c]))
            {
                return false;
            }
        }

        return true;
    }

    // The production decoder's view of a block: the run codec over an Arrow read of its bytes.
    private static (StructuralRow Key, StructuralRow Value, long Weight)[] Oracle(BlockSchema schema, TraceBlock block)
    {
        var stream = new byte[schema.SchemaMessage.Length + block.Message.Length];
        schema.SchemaMessage.CopyTo(stream, 0);
        block.Message.Span.CopyTo(stream.AsSpan(schema.SchemaMessage.Length));
        using var reader = new ArrowStreamReader(new MemoryStream(stream));
        using var batch = reader.ReadNextRecordBatch();
        return schema.Codec.DecodeEntries(batch);
    }

    public static TheoryData<int> Seeds() => new(Enumerable.Range(1, 40));

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task Block_DecodesLikeTheRunCodec_AndKeyEqualityAgreesWithEquals(int seed)
    {
        var rnd = new Random(seed);
        var keyKinds = Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => AllKinds[rnd.Next(AllKinds.Length)]).ToArray();
        var valueKinds = Enumerable.Range(0, rnd.Next(1, 6)).Select(_ => AllKinds[rnd.Next(AllKinds.Length)]).ToArray();
        var keySchema = new Schema([.. keyKinds.Select((k, c) => new SchemaColumn("k" + c, TypeOf(k, rnd.Next(3) > 0)))]);
        var valueSchema = new Schema([.. valueKinds.Select((k, c) => new SchemaColumn("v" + c, TypeOf(k, rnd.Next(3) > 0)))]);
        var schema = BlockSchema.TryCreate(keySchema, valueSchema)!;
        Assert.NotNull(schema);

        object? Gen(Schema s, CellKind kind, int c) => s[c].Type.Nullable && rnd.Next(7) == 0 ? null : Cell(kind, rnd);

        // Distinct keys, 1–3 values each, ordered by a hash that collides on purpose half the time
        // (few buckets), so equal-hash ranges hold several different keys.
        var collide = seed % 2 == 0;
        var keys = new Dictionary<StructuralRow, int>();
        for (var attempt = 0; attempt < 300; attempt++)
        {
            var key = new StructuralRow([.. keyKinds.Select((k, c) => Gen(keySchema, k, c))]);
            keys.TryAdd(key, collide ? key.GetHashCode() & 3 : key.GetHashCode());
        }

        var ordered = keys.OrderBy(kv => (uint)kv.Value).ToArray();
        var ks = new List<StructuralRow>();
        var vs = new List<StructuralRow>();
        var ws = new List<long>();
        var hs = new List<int>();
        foreach (var (key, hash) in ordered)
        {
            for (var v = rnd.Next(1, 4); v > 0; v--)
            {
                ks.Add(key);
                vs.Add(new StructuralRow([.. valueKinds.Select((k, c) => Gen(valueSchema, k, c))]));
                ws.Add(rnd.Next(2) == 0 ? rnd.Next(-5, 6) : long.MinValue + rnd.Next(3));
                hs.Add(hash);
            }
        }

        var block = schema.Encode([.. ks], [.. vs], [.. ws], [.. hs], ks.Count);
        var adopted = schema.Adopt(block.Message.Span);
        var viaArrow = schema.EncodeViaArrow([.. ks], [.. vs], [.. ws], [.. hs], ks.Count);

        // The header reader a fault adopts with (docs/design-bounded-memory.md §3.6) finds exactly
        // the buffers the Arrow reader finds, for blocks from either encoder.
        foreach (var source in new[] { block, viaArrow })
        {
            var reference = schema.Adopt(source.Message.Span);
            var header = TraceBlock.TryParseHeader(schema, source.Message.ToArray());
            Assert.NotNull(header);
            Assert.Equal(reference.Count, header.Count);
            for (var c = 0; c <= schema.HashColumn; c++)
            {
                Assert.Equal(reference.ValidityAt(c), header.ValidityAt(c));
                Assert.Equal(reference.ValuesAt(c), header.ValuesAt(c));
                Assert.Equal(reference.DataAt(c), header.DataAt(c));
            }
        }

        // The same entries encoded in 1–4 consecutive pieces and concatenated byte-wise.
        var cuts = Enumerable.Range(0, rnd.Next(0, 4)).Select(_ => rnd.Next(ks.Count + 1)).Append(0).Append(ks.Count).Distinct().Order().ToArray();
        var pieces = new List<TraceBlock>();
        for (var p = 0; p + 1 < cuts.Length; p++)
        {
            var (from, to) = (cuts[p], cuts[p + 1]);
            pieces.Add(schema.Encode([.. ks[from..to]], [.. vs[from..to]], [.. ws[from..to]], [.. hs[from..to]], to - from));
        }

        var concatenated = BlockWriter.Concat(schema, pieces);
        Assert.Equal(ks.Count, Oracle(schema, concatenated).Length);

        // Streamed to a file from the pieces, it is the same message byte for byte
        // (docs/design-bounded-memory.md §3d).
        var streamed = new CapturingFile();
        var length = await BlockWriter.ConcatToAsync(schema, pieces, streamed, default);
        Assert.Equal(concatenated.Message.Length, length);
        Assert.True(concatenated.Message.Span.SequenceEqual(streamed.Bytes.ToArray()), "streamed concatenation differs from Concat");
        var oracle = Oracle(schema, block);
        Assert.Equal(ks.Count, block.Count);
        Assert.Equal(ks.Count, oracle.Length);
        Assert.Equal((uint)hs[0], block.FirstHash);
        Assert.Equal((uint)hs[^1], block.LastHash);

        foreach (var b in new[] { block, adopted, viaArrow, concatenated, schema.Adopt(concatenated.Message.Span) })
        {
            for (var i = 0; i < b.Count; i++)
            {
                Assert.Equal((uint)hs[i], b.Hashes[i]);
                Assert.Equal(ws[i], b.Weight(i));
                Assert.Equal(ws[i], oracle[i].Weight);

                // The codec round-trips the original rows, and the block's cells agree with it.
                Check(CellwiseEquals(oracle[i].Key, ks[i]), () => $"codec key {oracle[i].Key} vs {ks[i]}");
                Check(CellwiseEquals(oracle[i].Value, vs[i]), () => $"codec value {oracle[i].Value} vs {vs[i]}");
                Check(CellwiseEquals(b.Key(i), oracle[i].Key), () => $"key {b.Key(i)} vs {oracle[i].Key}");
                Check(CellwiseEquals(b.Value(i), oracle[i].Value), () => $"value {b.Value(i)} vs {oracle[i].Value}");
                Assert.True(CellwiseEquals(b.Value(i, aliasStrings: true), oracle[i].Value));
                Assert.Equal(oracle[i].Key.GetHashCode(), b.Key(i).GetHashCode());
                Assert.Equal(oracle[i].Value.GetHashCode(), b.Value(i).GetHashCode());

                for (var c = 0; c < valueKinds.Length; c++)
                {
                    Assert.Equal(oracle[i].Value[c], b.ValueCell(i, c));
                }

                // Value hash and encoded value equality: with equal hashes, ValueEquals is row equality.
                Assert.Equal(oracle[i].Value.GetHashCode(), b.ValueHash(i));
                for (var m = 0; m < 4; m++)
                {
                    var cells = vs[i].ToArray();
                    var at = rnd.Next(cells.Length);
                    cells[at] = Mutate(cells[at], rnd);
                    foreach (var probe in new[] { vs[i], new StructuralRow(cells), vs[rnd.Next(vs.Count)] })
                    {
                        Check(
                            probe.Equals(oracle[i].Value) == (probe.GetHashCode() == b.ValueHash(i) && b.ValueEquals(i, probe)),
                            () => $"ValueEquals({i}, {probe}) disagrees with the decoded value {oracle[i].Value}");
                    }
                }

                var into = new object?[valueKinds.Length + 2];
                b.ValueCellsInto(i, into, 2);
                Assert.True(CellwiseEquals(new StructuralRow(into[2..]), oracle[i].Value));

                // Every stored key, and mutated probes, against every entry with the same hash.
                var probes = new List<StructuralRow> { ks[i], new([.. ks[i]]) };
                for (var m = 0; m < 6; m++)
                {
                    var cells = ks[i].ToArray();
                    var at = rnd.Next(cells.Length);
                    cells[at] = Mutate(cells[at], rnd);
                    probes.Add(new StructuralRow(cells));
                }

                probes.Add(new StructuralRow([.. ks[i], null]));
                probes.Add(ks[rnd.Next(ks.Count)]);
                for (var j = b.LowerBound(b.Hashes[i]); j < b.Count && b.Hashes[j] == b.Hashes[i]; j++)
                {
                    foreach (var probe in probes)
                    {
                        Check(
                            CellwiseEquals(probe, oracle[j].Key) == b.KeyEquals(j, probe),
                            () => $"KeyEquals({j}, {probe}) disagrees with the decoded key {oracle[j].Key}");
                    }
                }
            }
        }

        // LowerBound is the first entry not below the hash, for hashes in and around the block.
        for (var t = 0; t < 200; t++)
        {
            var h = rnd.Next(3) == 0 ? (uint)hs[rnd.Next(hs.Count)] + (uint)rnd.Next(-1, 2) : (uint)rnd.NextInt64(0, 1L << 32);
            var expected = hs.FindIndex(x => (uint)x >= h);
            Assert.Equal(expected < 0 ? block.Count : expected, block.LowerBound(h));
        }
    }

    [Fact]
    public void Strings_AreCopiedUnlessAliasIsAsked_SoKeptRowsDoNotPinTheBlock()
    {
        var schema = BlockSchema.TryCreate(
            new Schema([new SchemaColumn("k", new SqlIntegerType(false))]),
            new Schema([new SchemaColumn("s", new SqlVarcharType(null, false))]))!;
        var block = schema.Encode([new StructuralRow(1)], [new StructuralRow(Utf8String.Of("hello"))], [1], [new StructuralRow(1).GetHashCode()], 1);
        MemoryMarshal.TryGetArray(block.Message, out var blockBytes);

        var copied = (Utf8String)block.Value(0)[0]!;
        var aliased = (Utf8String)block.Value(0, aliasStrings: true)[0]!;
        Assert.Equal(Utf8String.Of("hello"), copied);
        Assert.Equal(Utf8String.Of("hello"), aliased);
        Assert.True(MemoryMarshal.TryGetArray(copied.Memory, out var c) && !ReferenceEquals(c.Array, blockBytes.Array));
        Assert.True(MemoryMarshal.TryGetArray(aliased.Memory, out var a) && ReferenceEquals(a.Array, blockBytes.Array));
    }

    [Fact]
    public void EmptyBlock_AndAllEmptyStrings_ReadWithoutAHeap()
    {
        var schema = BlockSchema.TryCreate(
            new Schema([new SchemaColumn("k", new SqlVarcharType(null, false))]),
            new Schema([new SchemaColumn("s", new SqlVarcharType(null, true))]))!;
        var empty = schema.Encode([], [], [], [], 0);
        Assert.Equal(0, empty.Count);
        Assert.Equal(0, TraceBlock.TryParseHeader(schema, empty.Message.ToArray())!.Count);
        Assert.Equal(0, empty.LowerBound(5));

        var key = new StructuralRow(Utf8String.Of(""));
        var block = schema.Encode([key, key], [new StructuralRow(Utf8String.Of("")), new StructuralRow((object?)null)], [1, 2], [key.GetHashCode(), key.GetHashCode()], 2);
        Assert.True(block.KeyEquals(0, key));
        Assert.False(block.KeyEquals(0, new StructuralRow((object?)null)));
        Assert.Equal(new StructuralRow(Utf8String.Of("")), block.Value(0));
        Assert.Equal(new StructuralRow((object?)null), block.Value(1));
    }

    [Fact]
    public void TypesWithoutABlockEncoding_KeepTheTraceManaged() =>
        Assert.Null(BlockSchema.TryCreate(
            new Schema([new SchemaColumn("k", new SqlIntegerType(false))]),
            new Schema([new SchemaColumn("i", new SqlIntervalType(IntervalQualifier.Year, true))])));

    // An ISequentialFile into memory.
    private sealed class CapturingFile : DbspNet.Core.IO.ISequentialFile
    {
        public MemoryStream Bytes { get; } = new();

        public long Position => Bytes.Length;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Bytes.Write(data.Span);
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void Dispose()
        {
        }
    }
}
