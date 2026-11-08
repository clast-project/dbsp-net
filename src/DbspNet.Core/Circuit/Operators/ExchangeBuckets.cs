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
