// Resident bytes of block state by operator, split by whether they could leave memory, and which of
// them a batch read (docs/design-bounded-memory.md §1). Finds every BlockedIndexedTrace and
// OrderedPartitionStore reachable from an operator through DbspNet-typed fields (a few levels; rows
// and collections are not walked), then classifies each block:
//   persisted   a block of a run whose run file a committed checkpoint wrote (or a restore adopted):
//               a durable copy exists, so dropping the bytes costs a re-read only
//   unpersisted a run block with no run file yet (built since the last checkpoint)
//   lazy_cold   a restored block never read: on disk only
//   chunk       a large window partition's chunk (OrderedPartitionStore; persisted another way)
// Read stamps (DBSPNET_BLOCK_ACCESS=1) count blocks read in the epoch by probe phase and by merges.
// Run from IvmMultiBatch with IVM_BLOCK_RESIDENCY=1.
using System.Globalization;
using System.Reflection;
using DbspNet.Core.Circuit;
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Persistence.Blocks;

namespace DbspNet.Tests.Scratch;

internal static class BlockResidency
{
    public sealed class Row
    {
        public required int Op;
        public required string Kind;
        public required string Holder;
        public long Traces, ManagedEntries, Runs, Blocks, Entries;
        public long PersistedBytes, UnpersistedBytes, LazyColdBytes, ChunkBytes;
        public long BornBytes; // built this epoch
        public long[] ReadBytes = new long[5]; // drain, write, ckpt, restore, merge
        public long ReadAnyBytes, ReadBlocks, ProbeReadBytes;
        public long SmallPartitions, SmallRows;

        public long Resident => PersistedBytes + UnpersistedBytes + ChunkBytes;
    }

    public static List<Row> Walk(IReadOnlyList<RootCircuit> replicas, object runner, int epoch)
    {
        var rows = new Dictionary<(int, string, string), Row>();
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        foreach (var r in replicas)
        {
            for (var i = 0; i < r.Operators.Count; i++)
            {
                var op = r.Operators[i];
                var kind = op is DbspNet.Core.Circuit.IIntrospectable m ? m.MetricName : op.GetType().Name;
                Visit(op, 4, "", (holder, o) => Charge(rows, i, kind, holder, o, epoch), seen);
            }
        }

        Visit(runner, 5, "", (holder, o) => Charge(rows, -1, "(runner)", holder, o, epoch), seen);
        return [.. rows.Values];
    }

    private static void Visit(object o, int depth, string path, Action<string, object> found, HashSet<object> seen)
    {
        if (!seen.Add(o))
        {
            return;
        }

        if (o is BlockedIndexedTrace or OrderedPartitionStore)
        {
            found(path.Length == 0 ? o.GetType().Name : path, o);
            return;
        }

        if (depth == 0)
        {
            return;
        }

        var t = o.GetType();
        if (o is Array arr)
        {
            if (arr.Length <= 256 && !t.GetElementType()!.IsValueType)
            {
                foreach (var e in arr)
                {
                    if (e is not null && Interesting(e.GetType()))
                    {
                        Visit(e, depth - 1, path + "[]", found, seen);
                    }
                }
            }

            return;
        }

        if (o is System.Collections.IList list && t.IsGenericType && list.Count <= 256)
        {
            foreach (var e in list)
            {
                if (e is not null && Interesting(e.GetType()))
                {
                    Visit(e, depth - 1, path + "[]", found, seen);
                }
            }

            return;
        }

        if (!Interesting(t))
        {
            return;
        }

        for (var ty = t; ty is not null && ty != typeof(object); ty = ty.BaseType)
        {
            foreach (var f in ty.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            {
                if (f.FieldType.IsValueType && !f.FieldType.IsGenericType)
                {
                    continue;
                }

                object? v;
                try
                {
                    v = f.GetValue(o);
                }
                catch
                {
                    continue;
                }

                if (v is null || v.GetType().IsValueType)
                {
                    continue;
                }

                if (v is System.Collections.IList || v is Array || Interesting(v.GetType()) || v is BlockedIndexedTrace or OrderedPartitionStore)
                {
                    Visit(v, depth - 1, (path.Length == 0 ? "" : path + ".") + f.Name, found, seen);
                }
            }
        }
    }

    // DbspNet types worth descending into: not rows, not Z-sets (their contents are rows).
    private static bool Interesting(Type t)
    {
        var ns = t.Namespace ?? "";
        if (!ns.StartsWith("DbspNet", StringComparison.Ordinal) || ns.StartsWith("DbspNet.Core.Collections", StringComparison.Ordinal))
        {
            return t.Name.StartsWith("<>c__DisplayClass", StringComparison.Ordinal) || t.Name.Contains("Closure", StringComparison.Ordinal);
        }

        return !t.IsSubclassOf(typeof(Delegate));
    }

    private static void Charge(Dictionary<(int, string, string), Row> rows, int op, string kind, string holder, object o, int epoch)
    {
        if (!rows.TryGetValue((op, kind, holder), out var row))
        {
            rows[(op, kind, holder)] = row = new Row { Op = op, Kind = kind, Holder = holder };
        }

        switch (o)
        {
            case BlockedIndexedTrace trace:
                Trace(row, trace, epoch);
                break;
            case OrderedPartitionStore store:
                var (parts, smallRows) = store.SmallManaged;
                row.SmallPartitions += parts;
                row.SmallRows += smallRows;
                foreach (var b in store.ChunkBlocks())
                {
                    Block(row, b, persisted: false, chunk: true, epoch);
                }

                if (store.ColdRecord is { } cold)
                {
                    Trace(row, cold, epoch);
                }

                break;
        }
    }

    private static void Trace(Row row, BlockedIndexedTrace trace, int epoch)
    {
        row.Traces++;
        row.ManagedEntries += trace.ManagedEntries;
        foreach (var run in trace.Runs)
        {
            row.Runs++;
            row.Entries += run.Entries;
            foreach (var b in run.Blocks)
            {
                Block(row, b, run.Persisted is not null, chunk: false, epoch);
            }
        }
    }

    private static void Block(Row row, ITraceBlock b, bool persisted, bool chunk, int epoch)
    {
        row.Blocks++;
        TraceBlock? resident = b switch
        {
            TraceBlock t => t,
            LazyTraceBlock l => l.Resident,
            _ => null,
        };
        if (resident is null)
        {
            row.LazyColdBytes += b is LazyTraceBlock lz ? lz.FileLength : 0;
            return;
        }

        var bytes = resident.ResidentBytes;
        if (chunk)
        {
            row.ChunkBytes += bytes;
        }
        else if (persisted)
        {
            row.PersistedBytes += bytes;
        }
        else
        {
            row.UnpersistedBytes += bytes;
        }

        var stamp = resident.Stamp;
        if (stamp.BornIn == epoch)
        {
            row.BornBytes += bytes;
        }

        var bits = stamp.BitsIn(epoch);
        if (bits != 0)
        {
            row.ReadBlocks++;
            row.ReadAnyBytes += bytes;
            if ((bits & 15) != 0)
            {
                row.ProbeReadBytes += bytes;
            }

            for (var k = 0; k < 5; k++)
            {
                if ((bits & (1 << k)) != 0)
                {
                    row.ReadBytes[k] += bytes;
                }
            }
        }
    }

    private static string Mb(long b) => (b / 1048576.0).ToString("F1", CultureInfo.InvariantCulture);

    public static IEnumerable<string> Report(string tag, List<Row> rows, int top = 30)
    {
        Row Sum(IEnumerable<Row> rs, int op, string kind, string holder)
        {
            var s = new Row { Op = op, Kind = kind, Holder = holder };
            foreach (var r in rs)
            {
                s.Traces += r.Traces; s.ManagedEntries += r.ManagedEntries; s.Runs += r.Runs; s.Blocks += r.Blocks; s.Entries += r.Entries;
                s.PersistedBytes += r.PersistedBytes; s.UnpersistedBytes += r.UnpersistedBytes; s.LazyColdBytes += r.LazyColdBytes; s.ChunkBytes += r.ChunkBytes;
                s.BornBytes += r.BornBytes; s.ReadAnyBytes += r.ReadAnyBytes; s.ReadBlocks += r.ReadBlocks; s.ProbeReadBytes += r.ProbeReadBytes;
                s.SmallPartitions += r.SmallPartitions; s.SmallRows += r.SmallRows;
                for (var k = 0; k < 5; k++)
                {
                    s.ReadBytes[k] += r.ReadBytes[k];
                }
            }

            return s;
        }

        string Line(string what, Row r) => string.Create(CultureInfo.InvariantCulture,
            $"RESID {tag} {what} resident_mb={Mb(r.Resident)} persisted_mb={Mb(r.PersistedBytes)} unpersisted_mb={Mb(r.UnpersistedBytes)} chunk_mb={Mb(r.ChunkBytes)} lazy_cold_mb={Mb(r.LazyColdBytes)} born_mb={Mb(r.BornBytes)} " +
            $"read_mb={Mb(r.ReadAnyBytes)} probe_read_mb={Mb(r.ProbeReadBytes)} read_drain/write/ckpt/restore/merge_mb={Mb(r.ReadBytes[0])}/{Mb(r.ReadBytes[1])}/{Mb(r.ReadBytes[2])}/{Mb(r.ReadBytes[3])}/{Mb(r.ReadBytes[4])} " +
            $"blocks={r.Blocks} read_blocks={r.ReadBlocks} runs={r.Runs} traces={r.Traces} run_entries={r.Entries} managed_entries={r.ManagedEntries} small_parts={r.SmallPartitions} small_rows={r.SmallRows}");

        yield return Line("TOTAL", Sum(rows, -2, "", ""));
        foreach (var g in rows.GroupBy(r => r.Kind).Select(g => Sum(g, -2, g.Key, "")).OrderByDescending(r => r.Resident + r.LazyColdBytes))
        {
            yield return Line("kind=" + g.Kind, g);
        }

        // Per operator position (summed over replicas).
        foreach (var g in rows.GroupBy(r => (r.Op, r.Kind, r.Holder)).Select(g => Sum(g, g.Key.Op, g.Key.Kind, g.Key.Holder))
                     .OrderByDescending(r => r.Resident + r.LazyColdBytes).Take(top))
        {
            yield return Line($"op={g.Op} kind={g.Kind} holder={g.Holder}", g);
        }
    }
}
