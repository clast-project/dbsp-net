// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Phase 2 of the durable-state arc (docs/design-durable-state.md §2 item 4): how many bytes would an
// incremental checkpoint write? For each pair of consecutive retained snapshots, per worker and per
// operator file, decode both through the operator's own codec, take the Z-set difference (new −
// old; keyed blobs: the entries that changed), and encode it with the same codec. The sum is what a
// batch-granular "manifest of delta runs" checkpoint would write — a lower bound for per-tick runs,
// which cannot cancel within the batch. Reported against the full snapshot's bytes.
using System.Globalization;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Scratch;

internal static class CheckpointDeltaProbe
{
    private sealed class Totals
    {
        public long FullBytes;
        public long DeltaBytes;
        public long FullEntries;
        public long DeltaEntries;
    }

    public static async Task<List<string>> MeasureAsync(ITableFileSystem fs, ParallelCompiledProgram program)
    {
        var lines = new List<string>();
        var dirs = (await Snapshot.ListSnapshotDirsAsync(fs, CancellationToken.None))
            .OrderBy(d => d.Tick).ThenBy(d => d.Gen).Select(d => Snapshot.SnapDirName(d.Tick, d.Gen)).ToList();
        for (var k = 1; k < dirs.Count; k++)
        {
            var byKind = new SortedDictionary<string, Totals>(StringComparer.Ordinal);
            var all = new Totals();
            for (var w = 0; w < program.Workers; w++)
            {
                foreach (var (op, spec) in program.ReshardSpecs.OrderBy(kv => kv.Key))
                {
                    foreach (var file in spec.Files)
                    {
                        var a = new TableFileSystemSnapshotContext(fs, $"{dirs[k - 1]}/worker-{w}/op-{op}");
                        var b = new TableFileSystemSnapshotContext(fs, $"{dirs[k]}/worker-{w}/op-{op}");
                        if (!await b.ExistsAsync(file.Name))
                        {
                            continue;
                        }

                        var (full, delta, fullEntries, deltaEntries) = await DiffAsync(fs, a, b, $"{dirs[k]}/worker-{w}/op-{op}/{file.Name}", file);
                        var kind = spec.OperatorType + "/" + file.Name;
                        if (!byKind.TryGetValue(kind, out var t))
                        {
                            t = new Totals();
                            byKind[kind] = t;
                        }

                        foreach (var x in new[] { t, all })
                        {
                            x.FullBytes += full;
                            x.DeltaBytes += delta;
                            x.FullEntries += fullEntries;
                            x.DeltaEntries += deltaEntries;
                        }
                    }
                }
            }

            lines.Add(Line("DELTA", dirs[k - 1], dirs[k], all));
            foreach (var (kind, t) in byKind.OrderByDescending(kv => kv.Value.DeltaBytes).Take(12))
            {
                lines.Add(Line("DELTAKIND " + kind, dirs[k - 1], dirs[k], t));
            }
        }

        return lines;
    }

    private static string Line(string label, string from, string to, Totals t) => string.Create(
        CultureInfo.InvariantCulture,
        $"{label} from={from} to={to} full_mib={t.FullBytes / 1048576.0:F1} delta_mib={t.DeltaBytes / 1048576.0:F2} " +
        $"ratio={(t.FullBytes == 0 ? 0 : (double)t.DeltaBytes / t.FullBytes):P3} full_entries={t.FullEntries} delta_entries={t.DeltaEntries}");

    private static async Task<(long Full, long Delta, long FullEntries, long DeltaEntries)> DiffAsync(
        ITableFileSystem fs, TableFileSystemSnapshotContext a, TableFileSystemSnapshotContext b, string bPath, ReshardFile file)
    {
        var full = (await fs.ReadAllBytesAsync(bPath)).LongLength;
        var aExists = await a.ExistsAsync(file.Name);
        var scratch = new InMemoryTableFileSystem();
        var sink = new TableFileSystemSnapshotContext(scratch, "d");
        switch (file)
        {
            case ZSetReshardFile z:
                {
                    var newer = await z.Codec.LoadAsync(b, z.Name);
                    var builder = new ZSetBuilder<StructuralRow, Z64>();
                    foreach (var (row, wt) in newer)
                    {
                        builder.Add(row, wt);
                    }

                    if (aExists)
                    {
                        foreach (var (row, wt) in await z.Codec.LoadAsync(a, z.Name))
                        {
                            builder.Add(row, -wt);
                        }
                    }

                    var diff = builder.Build();
                    await z.Codec.SaveAsync(sink, z.Name, diff);
                    return (full, (await scratch.ReadAllBytesAsync("d/" + z.Name)).LongLength, newer.Count, diff.Count);
                }

            case IndexedReshardFile ix:
                {
                    var newer = await ix.Codec.LoadAsync(b, ix.Name);
                    var builder = new IndexedZSetBuilder<StructuralRow, StructuralRow, Z64>();
                    long entries = 0;
                    foreach (var g in newer)
                    {
                        foreach (var (v, wt) in g.Value)
                        {
                            builder.Add(g.Key, v, wt);
                            entries++;
                        }
                    }

                    if (aExists)
                    {
                        foreach (var g in await ix.Codec.LoadAsync(a, ix.Name))
                        {
                            foreach (var (v, wt) in g.Value)
                            {
                                builder.Add(g.Key, v, -wt);
                            }
                        }
                    }

                    var diff = builder.Build();
                    long diffEntries = 0;
                    foreach (var g in diff)
                    {
                        diffEntries += g.Value.Count;
                    }

                    await ix.Codec.SaveAsync(sink, ix.Name, diff);
                    return (full, (await scratch.ReadAllBytesAsync("d/" + ix.Name)).LongLength, entries, diffEntries);
                }

            case KeyedBlobsReshardFile kb:
                {
                    var newer = await kb.Codec.LoadKeyedBlobsAsync(b, kb.Name);
                    var older = aExists
                        ? (await kb.Codec.LoadKeyedBlobsAsync(a, kb.Name)).ToDictionary(e => e.Key, e => e.Blob)
                        : [];
                    var changed = newer.Where(e => !older.TryGetValue(e.Key, out var blob) || !blob.AsSpan().SequenceEqual(e.Blob)).ToList();
                    var removed = older.Count - newer.Count(e => older.ContainsKey(e.Key));
                    await kb.Codec.SaveKeyedBlobsAsync(sink, kb.Name, changed);
                    return (full, (await scratch.ReadAllBytesAsync("d/" + kb.Name)).LongLength, newer.Count, changed.Count + removed);
                }

            default:
                throw new NotSupportedException(file.GetType().Name);
        }
    }
}
