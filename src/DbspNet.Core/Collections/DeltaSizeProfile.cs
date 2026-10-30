// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Runtime.CompilerServices;

namespace DbspNet.Core.Collections;

/// <summary>
/// Opt-in (<c>DBSPNET_DELTA_SIZE_PROFILE=1</c>) histogram of the dictionaries behind every Z-set
/// and indexed Z-set as it is formed: rows, and the bytes of its entry and bucket arrays, by the
/// operator stepping at the time. Sizes the case for delta containers that stay off the
/// large-object heap (docs/design-paged-trace-state.md §13). Attribution needs the circuit's
/// <c>ProfileOperators</c> on; anything formed outside an operator's step is "(outside step)".
/// </summary>
internal static class DeltaSizeProfile
{
    internal static readonly bool Enabled =
        Environment.GetEnvironmentVariable("DBSPNET_DELTA_SIZE_PROFILE") == "1";

    /// <summary>The large-object-heap threshold an array of references must stay under.</summary>
    internal const int LohBytes = 85_000;

    private static readonly long[] Edges = [LohBytes, 1L << 20, 4L << 20, 16L << 20];
    private static readonly object Gate = new();
    private static readonly Dictionary<string, Tally> ByOperator = new(StringComparer.Ordinal);

    [ThreadStatic]
    private static string? _current;

    /// <summary>Set by the circuit around each operator's step while profiling.</summary>
    internal static string? Current
    {
        get => _current;
        set => _current = value;
    }

    internal static void Record<TKey, TValue>(Dictionary<TKey, TValue> dictionary)
        where TKey : notnull
    {
        var rows = dictionary.Count;
        if (rows == 0)
        {
            return;
        }

        // Entry = (int hash, int next, key, value), padded to 8; buckets = one int per slot.
        var capacity = dictionary.EnsureCapacity(0);
        var entryBytes = (8 + Unsafe.SizeOf<TKey>() + Unsafe.SizeOf<TValue>() + 7) / 8 * 8;
        var entries = (long)capacity * entryBytes;
        var buckets = (long)capacity * sizeof(int);
        var name = _current ?? "(outside step)";
        lock (Gate)
        {
            if (!ByOperator.TryGetValue(name, out var t))
            {
                t = new Tally();
                ByOperator[name] = t;
            }

            t.Add(rows, entries, buckets);
        }
    }

    internal static void Reset()
    {
        lock (Gate)
        {
            ByOperator.Clear();
        }
    }

    /// <summary>Report lines: the whole histogram, then the operators putting the most entry
    /// bytes on the large-object heap.</summary>
    internal static IEnumerable<string> Report(int top = 20)
    {
        List<(string Name, Tally T)> rows;
        lock (Gate)
        {
            rows = ByOperator.Select(kv => (kv.Key, kv.Value.Copy())).ToList();
        }

        var all = new Tally();
        foreach (var (_, t) in rows)
        {
            all.Merge(t);
        }

        var labels = new[] { "<85K", "85K-1M", "1M-4M", "4M-16M", ">16M" };
        yield return "DELTASIZE entry-array bytes bucket: count / rows / entry bytes / bucket bytes";
        for (var b = 0; b < labels.Length; b++)
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"DELTASIZE   {labels[b],-7} n={all.Count[b],10:N0}  rows={all.Rows[b],14:N0}  entries={all.Entries[b] / 1e9,7:F2} GB  buckets={all.Buckets[b] / 1e9,6:F2} GB");
        }

        yield return string.Create(
            CultureInfo.InvariantCulture,
            $"DELTASIZE largest entry array {all.MaxEntries / 1e6:F1} MB ({all.MaxRows:N0} rows)");
        foreach (var (name, t) in rows.OrderByDescending(r => r.T.LohEntries).Take(top))
        {
            yield return string.Create(
                CultureInfo.InvariantCulture,
                $"DELTASIZE op {name,-36} loh n={t.LohCount,8:N0} entries={t.LohEntries / 1e9,6:F2} GB  max={t.MaxEntries / 1e6,6:F1} MB | all n={t.Count.Sum(),10:N0}");
        }
    }

    private sealed class Tally
    {
        public long[] Count { get; } = new long[5];

        public long[] Rows { get; } = new long[5];

        public long[] Entries { get; } = new long[5];

        public long[] Buckets { get; } = new long[5];

        public long MaxEntries { get; private set; }

        public long MaxRows { get; private set; }

        public long LohCount => Count.Skip(1).Sum();

        public long LohEntries => Entries.Skip(1).Sum();

        public void Add(long rows, long entries, long buckets)
        {
            var b = 0;
            while (b < Edges.Length && entries >= Edges[b])
            {
                b++;
            }

            Count[b]++;
            Rows[b] += rows;
            Entries[b] += entries;
            Buckets[b] += buckets;
            if (entries > MaxEntries)
            {
                MaxEntries = entries;
                MaxRows = rows;
            }
        }

        public void Merge(Tally other)
        {
            for (var b = 0; b < 5; b++)
            {
                Count[b] += other.Count[b];
                Rows[b] += other.Rows[b];
                Entries[b] += other.Entries[b];
                Buckets[b] += other.Buckets[b];
            }

            if (other.MaxEntries > MaxEntries)
            {
                MaxEntries = other.MaxEntries;
                MaxRows = other.MaxRows;
            }
        }

        public Tally Copy()
        {
            var t = new Tally();
            t.Merge(this);
            return t;
        }
    }
}
