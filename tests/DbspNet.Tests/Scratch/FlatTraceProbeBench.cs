// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
//
// FLAT_PROBE_BENCH=1: the phase-3 de-risk for docs/design-flat-trace-state.md §2.4 ("the row
// boundary"). One join probe pass (delta ⋈ trace, as IncrementalJoinCore.JoinInto does it, with
// the structural MergeRows combine and a ZSetBuilder) over the same trace held two ways:
//   A  today: IndexedZSet<StructuralRow, StructuralRow, Z64>, shared value rows;
//   B  v3 run blocks in managed byte[]s, probed by hash, each matching value rebuilt as a
//      StructuralRow (object[] + boxes, strings aliasing the block) and then combined;
//   B2 the same blocks, cells boxed straight into the output row's object[] (no value row).
// Reports ns and allocated bytes per output row, small and large deltas, arms alternating.
// FLAT_PROBE_KEYS (default 400000), FLAT_PROBE_ROUNDS (default 7).
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Clast.DatabaseDecimal.Values;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Persistence;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;
using Schema = DbspNet.Sql.Plan.Schema;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class FlatTraceProbeBench
{
    private readonly ITestOutputHelper _out;

    public FlatTraceProbeBench(ITestOutputHelper output) => _out = output;

    private enum Col { Long, Int, Double, Date, Decimal, Utf8 }

    private static readonly Schema KeySchema = new([new SchemaColumn("k", new SqlBigintType(false))]);

    private static readonly Schema ValueSchema = new([
        new SchemaColumn("id", new SqlBigintType(false)),
        new SchemaColumn("qty", new SqlIntegerType(true)),
        new SchemaColumn("name", new SqlVarcharType(null, true)),
        new SchemaColumn("code", new SqlVarcharType(null, true)),
        new SchemaColumn("price", new SqlDoubleType(true)),
        new SchemaColumn("d", new SqlDateType(true)),
        new SchemaColumn("amt", new SqlDecimalType(18, 2, true)),
        new SchemaColumn("note", new SqlVarcharType(null, true)),
    ]);

    private static readonly Col[] ValueCols = [Col.Long, Col.Int, Col.Utf8, Col.Utf8, Col.Double, Col.Date, Col.Decimal, Col.Utf8];

    private const int DeltaValueCols = 4;

    // One block: the RecordBatch message's body in a managed byte[], with each column's buffer
    // offsets (validity −1 when the column has no nulls in this block).
    private sealed class Block
    {
        public required byte[] Bytes;
        public required int Count;
        public required int[] Validity;
        public required int[] Values; // fixed-width values, or string offsets
        public required int[] Data;   // string heap (−1 otherwise)
    }

    [Fact]
    public void Run()
    {
        if (Environment.GetEnvironmentVariable("FLAT_PROBE_BENCH") != "1")
        {
            _out.WriteLine("FLAT_PROBE_BENCH not set — skipping.");
            return;
        }

        var keys = int.Parse(Environment.GetEnvironmentVariable("FLAT_PROBE_KEYS") ?? "400000", System.Globalization.CultureInfo.InvariantCulture);
        var rounds = int.Parse(Environment.GetEnvironmentVariable("FLAT_PROBE_ROUNDS") ?? "7", System.Globalization.CultureInfo.InvariantCulture);
        var rnd = new Random(42);

        // Shared boxes, as a built trace has them: pooled strings, one box each.
        var names = Enumerable.Range(0, 50_000).Select(i => (object)Utf8String.Of("customer-name-" + i)).ToArray();
        var codes = Enumerable.Range(0, 64).Select(i => (object)Utf8String.Of("C" + i)).ToArray();

        var tb = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
        var keyRows = new StructuralRow[keys];
        long entries = 0;
        for (var k = 0; k < keys; k++)
        {
            var key = new StructuralRow((long)k * 7919);
            keyRows[k] = key;
            var fan = 1 + rnd.Next(7);
            for (var f = 0; f < fan; f++)
            {
                var id = entries++;
                tb.Add(key, new StructuralRow(
                    id,
                    rnd.Next(20) == 0 ? null : rnd.Next(1000),
                    rnd.Next(20) == 0 ? null : names[rnd.Next(names.Length)],
                    codes[rnd.Next(codes.Length)],
                    rnd.NextDouble() * 1000,
                    new Date32(18000 + rnd.Next(3000)),
                    new Decimal128((Int128)rnd.Next(1_000_000)),
                    Utf8String.Of("note-" + id)), Z64.One);
            }
        }

        var trace = tb.Build();
        var (blocks, firstHash, lastHash) = BuildBlocks(trace);
        _out.WriteLine($"PROBE trace keys={keys} entries={entries} blocks={blocks.Length} block_bytes={blocks.Sum(b => (long)b.Bytes.Length) / 1e6:F1} MB");

        // Deltas: 90% hits, 10% misses; one value row each (4 columns, shared string box).
        IndexedZSet<StructuralRow, StructuralRow, Z64> Delta(int n)
        {
            var b = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
            for (var i = 0; i < n; i++)
            {
                var key = rnd.Next(10) == 0 ? new StructuralRow(-1L - rnd.Next(1_000_000)) : keyRows[rnd.Next(keys)];
                b.Add(key, new StructuralRow((long)i, codes[rnd.Next(codes.Length)], rnd.Next(100), rnd.NextDouble()), Z64.One);
            }

            return b.Build();
        }

        var regimes = new (string Name, IndexedZSet<StructuralRow, StructuralRow, Z64>[] Deltas)[]
        {
            ("small", Enumerable.Range(0, 100).Select(_ => Delta(2_000)).ToArray()),
            ("large", Enumerable.Range(0, 2).Select(_ => Delta(200_000)).ToArray()),
        };

        // Correctness first: all three arms produce the same Z-set.
        foreach (var (name, deltas) in regimes)
        {
            var a = RunA(deltas[0], trace);
            var b = RunB(deltas[0], blocks, firstHash, lastHash, direct: false);
            var b2 = RunB(deltas[0], blocks, firstHash, lastHash, direct: true);
            Assert.True(a.Equals(b), $"{name}: B differs from A");
            Assert.True(a.Equals(b2), $"{name}: B2 differs from A");
            _out.WriteLine($"PROBE {name} delta_keys={deltas[0].GroupCount} out_rows={a.Count} (arms agree)");
        }

        var arms = new[] { "A", "B", "B2", "PA", "PB" };
        foreach (var (name, deltas) in regimes)
        {
            var ns = arms.ToDictionary(x => x, _ => new List<double>());
            var bytes = arms.ToDictionary(x => x, _ => new List<double>());
            for (var r = -1; r < rounds; r++)
            {
                // Alternate the arm order each round.
                foreach (var arm in r % 2 == 0 ? arms : arms.Reverse().ToArray())
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    var a0 = GC.GetAllocatedBytesForCurrentThread();
                    var sw = Stopwatch.StartNew();
                    long outRows = 0;
                    foreach (var d in deltas)
                    {
                        outRows += arm switch
                        {
                            "A" => RunA(d, trace).Count,
                            "B" => RunB(d, blocks, firstHash, lastHash, direct: false).Count,
                            "B2" => RunB(d, blocks, firstHash, lastHash, direct: true).Count,
                            "PA" => ProbeA(d, trace),
                            _ => ProbeB(d, blocks, firstHash, lastHash),
                        };
                    }

                    sw.Stop();
                    var alloc = GC.GetAllocatedBytesForCurrentThread() - a0;
                    if (r >= 0)
                    {
                        ns[arm].Add(sw.Elapsed.TotalNanoseconds / outRows);
                        bytes[arm].Add((double)alloc / outRows);
                    }
                }
            }

            foreach (var arm in arms)
            {
                var sorted = ns[arm].Order().ToArray();
                _out.WriteLine(
                    $"PROBE {name} arm={arm} ns_per_out_median={sorted[sorted.Length / 2]:F1} " +
                    $"range={sorted[0]:F1}-{sorted[^1]:F1} bytes_per_out={bytes[arm].Average():F1}");
            }
        }
    }

    private static StructuralRow Merge(StructuralRow left, StructuralRow right)
    {
        var vs = new object?[left.Count + right.Count];
        for (var i = 0; i < left.Count; i++)
        {
            vs[i] = left[i];
        }

        for (var i = 0; i < right.Count; i++)
        {
            vs[left.Count + i] = right[i];
        }

        return new StructuralRow(vs);
    }

    private static ZSet<StructuralRow, Z64> RunA(
        IndexedZSet<StructuralRow, StructuralRow, Z64> delta, IndexedZSet<StructuralRow, StructuralRow, Z64> trace)
    {
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        DbspNet.Core.Operators.Stateful.IncrementalJoinCore.JoinInto(
            delta, trace, static (_, l, r) => Merge(l, r), null, builder);
        return builder.Build();
    }

    // Probe only: matched (delta value, trace entry) pairs, no rows built — the lookup cost alone.
    private static long ProbeA(
        IndexedZSet<StructuralRow, StructuralRow, Z64> delta, IndexedZSet<StructuralRow, StructuralRow, Z64> trace)
    {
        long n = 0;
        foreach (var (key, dgroup) in delta)
        {
            var g = trace.GroupFor(key);
            foreach (var (_, w) in g)
            {
                n += dgroup.Count * (w.Value != 0 ? 1 : 0);
            }
        }

        return n;
    }

    private static long ProbeB(
        IndexedZSet<StructuralRow, StructuralRow, Z64> delta, Block[] blocks, uint[] firstHash, uint[] lastHash)
    {
        long n = 0;
        var weightCol = 1 + ValueCols.Length;
        var hashCol = weightCol + 1;
        foreach (var (key, dgroup) in delta)
        {
            var h = (uint)key.GetHashCode();
            var bi = UpperBound(firstHash, h) - 1;
            if (bi < 0 || lastHash[bi] < h)
            {
                continue;
            }

            var blk = blocks[bi];
            var hashes = MemoryMarshal.Cast<byte, uint>(blk.Bytes.AsSpan(blk.Values[hashCol], blk.Count * 4));
            var keyVals = MemoryMarshal.Cast<byte, long>(blk.Bytes.AsSpan(blk.Values[0], blk.Count * 8));
            var weights = MemoryMarshal.Cast<byte, long>(blk.Bytes.AsSpan(blk.Values[weightCol], blk.Count * 8));
            var probe = (long)key[0]!;
            for (var at = LowerBound(hashes, h); at < blk.Count && hashes[at] == h; at++)
            {
                if (keyVals[at] == probe)
                {
                    n += dgroup.Count * (weights[at] != 0 ? 1 : 0);
                }
            }
        }

        return n;
    }

    private static ZSet<StructuralRow, Z64> RunB(
        IndexedZSet<StructuralRow, StructuralRow, Z64> delta, Block[] blocks, uint[] firstHash, uint[] lastHash, bool direct)
    {
        var builder = new ZSetBuilder<StructuralRow, Z64>();
        var keyCol = 0;
        var weightCol = 1 + ValueCols.Length;
        var hashCol = weightCol + 1;
        foreach (var (key, dgroup) in delta)
        {
            var h = (uint)key.GetHashCode();
            var bi = UpperBound(firstHash, h) - 1;
            if (bi < 0 || lastHash[bi] < h)
            {
                continue;
            }

            var blk = blocks[bi];
            var hashes = MemoryMarshal.Cast<byte, uint>(blk.Bytes.AsSpan(blk.Values[hashCol], blk.Count * 4));
            var at = LowerBound(hashes, h);
            var keyVals = MemoryMarshal.Cast<byte, long>(blk.Bytes.AsSpan(blk.Values[keyCol], blk.Count * 8));
            var weights = MemoryMarshal.Cast<byte, long>(blk.Bytes.AsSpan(blk.Values[weightCol], blk.Count * 8));
            var probe = (long)key[0]!;
            for (; at < blk.Count && hashes[at] == h; at++)
            {
                if (keyVals[at] != probe)
                {
                    continue;
                }

                var w = new Z64(weights[at]);
                if (direct)
                {
                    foreach (var (dv, dw) in dgroup)
                    {
                        var vs = new object?[DeltaValueCols + ValueCols.Length];
                        for (var c = 0; c < DeltaValueCols; c++)
                        {
                            vs[c] = dv[c];
                        }

                        for (var c = 0; c < ValueCols.Length; c++)
                        {
                            vs[DeltaValueCols + c] = Cell(blk, 1 + c, ValueCols[c], at);
                        }

                        builder.Add(new StructuralRow(vs), Z64.Multiply(dw, w));
                    }
                }
                else
                {
                    var cells = new object?[ValueCols.Length];
                    for (var c = 0; c < ValueCols.Length; c++)
                    {
                        cells[c] = Cell(blk, 1 + c, ValueCols[c], at);
                    }

                    var value = new StructuralRow(cells);
                    foreach (var (dv, dw) in dgroup)
                    {
                        builder.Add(Merge(dv, value), Z64.Multiply(dw, w));
                    }
                }
            }
        }

        return builder.Build();
    }

    private static object? Cell(Block b, int col, Col kind, int i)
    {
        var v = b.Validity[col];
        if (v >= 0 && (b.Bytes[v + (i >> 3)] & (1 << (i & 7))) == 0)
        {
            return null;
        }

        var o = b.Values[col];
        var s = b.Bytes.AsSpan();
        switch (kind)
        {
            case Col.Long: return BinaryPrimitives.ReadInt64LittleEndian(s[(o + (i * 8))..]);
            case Col.Int: return BinaryPrimitives.ReadInt32LittleEndian(s[(o + (i * 4))..]);
            case Col.Double: return BinaryPrimitives.ReadDoubleLittleEndian(s[(o + (i * 8))..]);
            case Col.Date: return new Date32(BinaryPrimitives.ReadInt32LittleEndian(s[(o + (i * 4))..]));
            case Col.Decimal: return new Decimal128(MemoryMarshal.Read<Int128>(s[(o + (i * 16))..]));
            default:
                var start = BinaryPrimitives.ReadInt32LittleEndian(s[(o + (i * 4))..]);
                var end = BinaryPrimitives.ReadInt32LittleEndian(s[(o + ((i + 1) * 4))..]);
                return Utf8String.FromBytes(new ReadOnlyMemory<byte>(b.Bytes, b.Data[col] + start, end - start));
        }
    }

    private static int UpperBound(uint[] a, uint h)
    {
        int lo = 0, hi = a.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (a[mid] <= h) lo = mid + 1; else hi = mid;
        }

        return lo;
    }

    private static int LowerBound(ReadOnlySpan<uint> a, uint h)
    {
        int lo = 0, hi = a.Length;
        while (lo < hi)
        {
            var mid = (lo + hi) >>> 1;
            if (a[mid] < h) lo = mid + 1; else hi = mid;
        }

        return lo;
    }

    // Cut the trace into v3 blocks (hash order, ≥1024 entries, cut only at a key boundary), encode
    // each with the production codec, serialise it as an IPC stream into a managed byte[], and
    // keep only buffer offsets: Arrow's reader over ReadOnlyMemory aliases the array.
    private static (Block[] Blocks, uint[] FirstHash, uint[] LastHash) BuildBlocks(
        IndexedZSet<StructuralRow, StructuralRow, Z64> trace)
    {
        var codec = new ArrowIndexedZSetTraceCodec(KeySchema, ValueSchema);
        var groups = trace.Select(g => (Hash: (uint)g.Key.GetHashCode(), g.Key, g.Value)).OrderBy(g => g.Hash).ToArray();
        var blocks = new List<Block>();
        var first = new List<uint>();
        var last = new List<uint>();
        var ks = new List<StructuralRow>();
        var vs = new List<StructuralRow>();
        var ws = new List<long>();
        var hs = new List<int>();
        for (var g = 0; g < groups.Length; g++)
        {
            foreach (var (v, w) in groups[g].Value)
            {
                ks.Add(groups[g].Key);
                vs.Add(v);
                ws.Add(w.Value);
                hs.Add((int)groups[g].Hash);
            }

            var boundary = g == groups.Length - 1 || groups[g + 1].Hash != groups[g].Hash;
            if (boundary && (ks.Count >= 1024 || g == groups.Length - 1))
            {
                using var batch = codec.EncodeBlock([.. ks], [.. vs], [.. ws], [.. hs], ks.Count);
                blocks.Add(Parse(batch, codec.RunSchema));
                first.Add((uint)hs[0]);
                last.Add((uint)hs[^1]);
                ks.Clear();
                vs.Clear();
                ws.Clear();
                hs.Clear();
            }
        }

        return (blocks.ToArray(), first.ToArray(), last.ToArray());
    }

    private static Block Parse(RecordBatch batch, Apache.Arrow.Schema schema)
    {
        var ms = new MemoryStream();
        using (var w = new ArrowStreamWriter(ms, schema, leaveOpen: true))
        {
            w.WriteRecordBatch(batch);
        }

        var bytes = ms.ToArray();
        using var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(bytes));
        using var read = reader.ReadNextRecordBatch();
        var n = read.ColumnCount;
        var validity = new int[n];
        var values = new int[n];
        var data = new int[n];
        for (var c = 0; c < n; c++)
        {
            var arr = ((Apache.Arrow.Array)read.Column(c)).Data;
            Assert.Equal(0, arr.Offset);
            int Off(int i)
            {
                var buf = arr.Buffers[i];
                if (buf.Length == 0)
                {
                    return -1;
                }

                Assert.True(MemoryMarshal.TryGetArray(buf.Memory, out var seg) && ReferenceEquals(seg.Array, bytes), "IPC read did not alias the byte[]");
                return seg.Offset;
            }

            validity[c] = arr.NullCount == 0 ? -1 : Off(0);
            values[c] = Off(1);
            data[c] = arr.Buffers.Length > 2 ? Off(2) : -1;
        }

        return new Block { Bytes = bytes, Count = read.Length, Validity = validity, Values = values, Data = data };
    }
}
