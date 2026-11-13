// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// The exchange operators' per-destination buckets: a <see cref="List{T}"/>, or a
/// <see cref="ChunkedList{T}"/> (the default; <c>DBSPNET_ZSET_MAP=dictionary</c> opts out) — a bucket of a bulk-load
/// delta grows past the large-object threshold as a List (docs/design-chunked-deltas.md,
/// increment 4). Reads go through <see cref="Items"/>, which enumerates either without boxing.
/// </summary>
internal static class ExchangeBuckets
{
    public static void Add<T>(ref IReadOnlyList<T>? bucket, T item)
    {
        if (bucket is not null && bucket.GetType() == typeof(List<T>))
        {
            Unsafe.As<List<T>>(bucket).Add(item);
            return;
        }

        if (bucket is null)
        {
            if (ZSetMapMode.Chunked)
            {
                var chunked = new ChunkedList<T>();
                chunked.Add(item);
                bucket = chunked;
            }
            else
            {
                bucket = new List<T> { item };
            }

            return;
        }

        Unsafe.As<ChunkedList<T>>(bucket).Add(item);
    }

    public static Items<T> Items<T>(IReadOnlyList<T> bucket) => new(bucket);

    /// <summary>
    /// Split a block-backed delta into one builder per destination, each spilling as the delta does at its share of the step chunk (so the split holds one chunk managed, not one per destination) (docs/design-bounded-memory.md
    /// §5.27, §5.32), so a large bucket stays blocks. Each non-empty bucket is a <see cref="SetBucket{TKey,TWeight}"/>,
    /// which a gather enumerates directly (<see cref="Items{T}"/> reads only the list buckets).
    /// </summary>
    public static long SplitSpilling<TKey, TWeight>(
        ZSet<TKey, TWeight> input,
        IReadOnlyList<KeyValuePair<TKey, TWeight>>?[] buckets,
        Func<TKey, int> partition)
        where TKey : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        var external = input.External ?? throw new ArgumentException("a block-backed delta", nameof(input));
        var workers = buckets.Length;
        var builders = new ZSetBuilder<TKey, TWeight>?[workers];
        long rows = 0;
        foreach (var kv in input)
        {
            var j = ((partition(kv.Key) % workers) + workers) % workers;
            (builders[j] ??= external.NewBuilder(0, workers)).Add(kv.Key, kv.Value);
            rows++;
        }

        for (var j = 0; j < workers; j++)
        {
            if (builders[j] is { } b && b.Build() is { IsEmpty: false } set)
            {
                buckets[j] = new SetBucket<TKey, TWeight>(set);
            }
        }

        return rows;
    }

    /// <summary>
    /// Split a block-backed delta into one grouping builder per destination (docs/design-bounded-memory.md
    /// §5.41): each row keyed by <paramref name="keyOf"/> here, where it is decoded anyway, so the gather
    /// unions the grouped runs (<see cref="GatherGrouped"/>) instead of decoding every row a second time to
    /// key it. Each builder spills at its share of the step chunk. Each non-empty bucket is a
    /// <see cref="GroupedBucket{TKey,TRow,TWeight}"/>. Null, and nothing published, unless the rows are
    /// structural and <paramref name="format"/> a block format.
    /// </summary>
    public static long? SplitGrouping<TKey, TRow, TWeight>(
        ZSet<TRow, TWeight> input,
        IReadOnlyList<KeyValuePair<TRow, TWeight>>?[] buckets,
        Func<TRow, int> partition,
        Func<TRow, TKey> keyOf,
        object? format)
        where TKey : notnull
        where TRow : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        if (!GroupedSplits || typeof(TKey) != typeof(StructuralRow) || typeof(TRow) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64)
            || format is not DbspNet.Core.Operators.Stateful.Blocks.ITraceBlockFormat
            || DbspNet.Core.Operators.Stateful.Blocks.SpillSettings.StepSpillRows is null)
        {
            return null;
        }

        var workers = buckets.Length;
        var builders = new IndexedZSetBuilder<TKey, TRow, TWeight>?[workers];
        var counts = new int[workers];
        long rows = 0;
        foreach (var kv in input)
        {
            var j = ((partition(kv.Key) % workers) + workers) % workers;
            (builders[j] ??= DbspNet.Core.Operators.Stateful.Blocks.DeltaSpillGroups.For<TKey, TRow, TWeight>(format, workers))
                .Add(keyOf(kv.Key), kv.Key, kv.Value);
            counts[j]++;
            rows++;
        }

        for (var j = 0; j < workers; j++)
        {
            if (builders[j] is { } b && b.Build() is { IsEmpty: false } set)
            {
                buckets[j] = new GroupedBucket<TKey, TRow, TWeight>(set, counts[j]);
            }
        }

        return rows;
    }

    /// <summary>
    /// <see cref="SplitGrouping{TKey,TRow,TWeight}"/> with a fused pointwise pre-step (docs/design-bounded-memory.md
    /// §5.55): each decoded row is first rewritten or dropped by <paramref name="step"/>, then partitioned, keyed and
    /// grouped by its rewritten form, so the upstream map/filter never builds a delta of its own. Two input rows the
    /// step maps to one row land in the same builder (they partition alike) and merge there, or in the gather's union.
    /// Null, and nothing published, under the same conditions.
    /// </summary>
    public static long? SplitGrouping<TKey, TIn, TRow, TWeight>(
        ZSet<TIn, TWeight> input,
        IReadOnlyList<KeyValuePair<TRow, TWeight>>?[] buckets,
        Func<TIn, (bool Keep, TRow Row)> step,
        Func<TRow, int> partition,
        Func<TRow, TKey> keyOf,
        object? format)
        where TKey : notnull
        where TIn : notnull
        where TRow : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        if (!GroupedSplits || typeof(TKey) != typeof(StructuralRow) || typeof(TRow) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64)
            || format is not DbspNet.Core.Operators.Stateful.Blocks.ITraceBlockFormat
            || DbspNet.Core.Operators.Stateful.Blocks.SpillSettings.StepSpillRows is null)
        {
            return null;
        }

        var workers = buckets.Length;
        var builders = new IndexedZSetBuilder<TKey, TRow, TWeight>?[workers];
        var counts = new int[workers];
        long rows = 0;
        foreach (var kv in input)
        {
            var (keep, row) = step(kv.Key);
            if (!keep)
            {
                continue;
            }

            var j = ((partition(row) % workers) + workers) % workers;
            (builders[j] ??= DbspNet.Core.Operators.Stateful.Blocks.DeltaSpillGroups.For<TKey, TRow, TWeight>(format, workers))
                .Add(keyOf(row), row, kv.Value);
            counts[j]++;
            rows++;
        }

        for (var j = 0; j < workers; j++)
        {
            if (builders[j] is { } b && b.Build() is { IsEmpty: false } set)
            {
                buckets[j] = new GroupedBucket<TKey, TRow, TWeight>(set, counts[j]);
            }
        }

        return rows;
    }

    /// <summary>Whether an exchange-index split keys and groups a block-backed delta's rows (§5.41). Off
    /// for A/B (<c>DBSPNET_GROUPED_SPLITS=off</c>).</summary>
    internal static bool GroupedSplits { get; set; } =
        !string.Equals(Environment.GetEnvironmentVariable("DBSPNET_GROUPED_SPLITS"), "off", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// An exchange-index gather's column when a bucket is a grouped block-backed set of
    /// <paramref name="format"/> (§5.41): the sum is the union of those sets' runs, no row decoded. The other
    /// buckets (rows from a managed split, or a small grouped set kept managed) are keyed into a blocked
    /// builder and unioned too. Null when no bucket is grouped and block backed. <paramref name="rows"/>
    /// counts the rows keyed here.
    /// </summary>
    public static IndexedZSet<TKey, TRow, TWeight>? GatherGrouped<TKey, TRow, TWeight>(
        IReadOnlyList<KeyValuePair<TRow, TWeight>>?[] column, Func<TRow, TKey> keyOf, object? format, out long rows)
        where TKey : notnull
        where TRow : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        rows = 0;
        if (typeof(TKey) != typeof(StructuralRow) || typeof(TRow) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64)
            || format is not DbspNet.Core.Operators.Stateful.Blocks.ITraceBlockFormat blockFormat)
        {
            return null;
        }

        static DbspNet.Core.Operators.Stateful.Blocks.BlockedGroups? Blocked(object? bucket, object format) =>
            bucket is GroupedBucket<StructuralRow, StructuralRow, Z64> { Set.External: DbspNet.Core.Operators.Stateful.Blocks.BlockedGroups g }
            && ReferenceEquals(g.Format, format) ? g : null;

        var blocked = new List<DbspNet.Core.Operators.Stateful.Blocks.BlockedGroups>();
        var rest = false;
        foreach (var bucket in column)
        {
            if (Blocked(bucket, blockFormat) is { } g)
            {
                blocked.Add(g);
            }
            else
            {
                rest |= bucket is not null;
            }
        }

        if (blocked.Count == 0)
        {
            return null;
        }

        var key = Unsafe.As<Func<StructuralRow, StructuralRow>>(keyOf);
        if (rest)
        {
            var builder = DbspNet.Core.Operators.Stateful.Blocks.DeltaSpillGroups.Blocked(blockFormat);
            foreach (var bucket in column)
            {
                if (bucket is null || Blocked(bucket, blockFormat) is not null)
                {
                    continue;
                }

                if (bucket is GroupedBucket<StructuralRow, StructuralRow, Z64> grouped)
                {
                    foreach (var (k, group) in grouped.Set)
                    {
                        foreach (var (row, weight) in group)
                        {
                            builder.Add(k, row, weight);
                            rows++;
                        }
                    }
                }
                else if (bucket is SetBucket<StructuralRow, Z64> set)
                {
                    foreach (var (row, weight) in set.Set)
                    {
                        builder.Add(key(row), row, weight);
                        rows++;
                    }
                }
                else
                {
                    foreach (var (row, weight) in Items(Unsafe.As<IReadOnlyList<KeyValuePair<StructuralRow, Z64>>>(bucket)))
                    {
                        builder.Add(key(row), row, weight);
                        rows++;
                    }
                }
            }

            if (builder.Build().External is DbspNet.Core.Operators.Stateful.Blocks.BlockedGroups g)
            {
                blocked.Add(g);
            }
        }

        var union = DbspNet.Core.Operators.Stateful.Blocks.BlockedGroups.Union(blocked, blockFormat);
        return Unsafe.As<IndexedZSet<TKey, TRow, TWeight>>(union);
    }

    /// <summary>
    /// A gathered column whose buckets are block-backed sets of <paramref name="format"/>, summed as the
    /// union of their runs instead of decoded into a builder and encoded again (docs/design-bounded-memory.md
    /// §5.35: a skewed exchange lands millions of spilled rows on one worker). Any other buckets go through a
    /// spilling builder and are added. Null when the rows are not structural or no bucket is block backed.
    /// </summary>
    public static ZSet<TKey, TWeight>? GatherBlocked<TKey, TWeight>(
        IReadOnlyList<KeyValuePair<TKey, TWeight>>?[] column, object format)
        where TKey : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        if (typeof(TKey) != typeof(StructuralRow) || typeof(TWeight) != typeof(Z64)
            || format is not DbspNet.Core.Operators.Stateful.Blocks.ITraceBlockFormat blockFormat)
        {
            return null;
        }

        var blocked = new List<DbspNet.Core.Operators.Stateful.Blocks.BlockedRows>();
        long rest = 0;
        foreach (var bucket in column)
        {
            if (bucket is SetBucket<TKey, TWeight> { Set.External: DbspNet.Core.Operators.Stateful.Blocks.BlockedRows b }
                && ReferenceEquals(b.Format, blockFormat))
            {
                blocked.Add(b);
            }
            else
            {
                rest += bucket?.Count ?? 0;
            }
        }

        if (blocked.Count == 0)
        {
            return null;
        }

        var sum = DbspNet.Core.Operators.Stateful.Blocks.BlockedRows.Union(blocked, blockFormat);
        if (rest > 0)
        {
            var builder = DbspNet.Core.Operators.Stateful.Blocks.DeltaSpill.Rows(format, (int)Math.Min(rest, int.MaxValue));
            foreach (var bucket in column)
            {
                if (bucket is not null && !(bucket is SetBucket<TKey, TWeight> { Set.External: DbspNet.Core.Operators.Stateful.Blocks.BlockedRows b }
                    && ReferenceEquals(b.Format, blockFormat)))
                {
                    builder.AddRange(Unsafe.As<IReadOnlyList<KeyValuePair<StructuralRow, Z64>>>(bucket));
                }
            }

            sum += builder.Build();
        }

        return Unsafe.As<ZSet<TKey, TWeight>>(sum);
    }

    /// <summary>A built (possibly block-backed) Z-set published as a bucket: a gather only enumerates it.</summary>
    internal sealed class SetBucket<TKey, TWeight>(ZSet<TKey, TWeight> set) : IReadOnlyList<KeyValuePair<TKey, TWeight>>
        where TKey : notnull
        where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
    {
        public ZSet<TKey, TWeight> Set => set;

        public int Count => set.Count;

        public KeyValuePair<TKey, TWeight> this[int index] => throw new NotSupportedException("a spilled exchange bucket is enumerated, not indexed");

        public IEnumerator<KeyValuePair<TKey, TWeight>> GetEnumerator() => set.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}

/// <summary>A split's grouped (possibly block-backed) indexed set published as a bucket (§5.41): an
/// exchange-index gather unions it; read as rows it is its entries.</summary>
internal sealed class GroupedBucket<TKey, TRow, TWeight>(IndexedZSet<TKey, TRow, TWeight> set, int count) : IReadOnlyList<KeyValuePair<TRow, TWeight>>
    where TKey : notnull
    where TRow : notnull
    where TWeight : struct, DbspNet.Core.Algebra.IZRing<TWeight>
{
    public IndexedZSet<TKey, TRow, TWeight> Set => set;

    /// <summary>The rows the split added (before consolidation).</summary>
    public int Count => count;

    public KeyValuePair<TRow, TWeight> this[int index] => throw new NotSupportedException("a grouped exchange bucket is enumerated, not indexed");

    public IEnumerator<KeyValuePair<TRow, TWeight>> GetEnumerator()
    {
        foreach (var (_, group) in set)
        {
            foreach (var kv in group)
            {
                yield return kv;
            }
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>A bucket enumerated by pattern — either list's struct enumerator, unboxed.</summary>
internal readonly struct Items<T>(IReadOnlyList<T> bucket)
{
    public Enumerator GetEnumerator() => new(bucket);

    public struct Enumerator
    {
        private readonly bool _isList;
        private List<T>.Enumerator _list;
        private ChunkedList<T>.Enumerator _chunked;

        internal Enumerator(IReadOnlyList<T> bucket)
        {
            _isList = bucket.GetType() == typeof(List<T>);
            _list = _isList ? Unsafe.As<List<T>>(bucket).GetEnumerator() : default;
            _chunked = _isList ? default : Unsafe.As<ChunkedList<T>>(bucket).GetEnumerator();
        }

        public readonly T Current => _isList ? _list.Current : _chunked.Current;

        public bool MoveNext() => _isList ? _list.MoveNext() : _chunked.MoveNext();
    }
}
