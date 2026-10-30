// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections;
using System.Runtime.CompilerServices;
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
