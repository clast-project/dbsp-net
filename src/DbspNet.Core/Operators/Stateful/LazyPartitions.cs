// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// A partitioned operator's saved rows, held cold after a lazy restore (docs/design-durable-state.md
/// §1f, increment 5). The rows were written in order of their partition key's hash, so one
/// partition is one hash: <see cref="ReadByHash"/> returns every row stored under it (possibly with
/// rows of other partitions that share the hash, which the caller filters out).
/// </summary>
internal interface IColdRows<TRow>
    where TRow : notnull
{
    /// <summary>Rows held (the state's exact size when it was saved).</summary>
    long Count { get; }

    ZSet<TRow, Z64> ReadByHash(int hash);

    IEnumerable<KeyValuePair<TRow, Z64>> ReadAll();
}

/// <summary>
/// A trace's saved rows, held cold after a lazy restore (docs/design-durable-state.md §1f,
/// increment 6): read one row at a time by the row itself, or all at once.
/// </summary>
internal interface IColdZSet<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Rows held — exact, known without reading any.</summary>
    int Count { get; }

    TWeight WeightOf(TKey key);

    IEnumerable<KeyValuePair<TKey, TWeight>> ReadAll();
}

/// <summary>A codec that can hand a saved trace back cold (null when the file is not a
/// row-ordered chain, or lazy restore is off).</summary>
internal interface IColdZSetLoader<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    ValueTask<IColdZSet<TKey, TWeight>?> LoadColdZSetAsync(ISnapshotReader reader, string fileName, CancellationToken cancellationToken);
}

/// <summary>A codec that can hand back a saved row Z-set cold instead of reading it (null when the
/// file was not saved partition-ordered, or lazy restore is off).</summary>
internal interface IColdRowLoader<TRow>
    where TRow : notnull
{
    ValueTask<IColdRows<TRow>?> LoadColdAsync(ISnapshotReader reader, string fileName, CancellationToken cancellationToken);
}

/// <summary>
/// An operator whose snapshot file is saved in an order other than its rows' own hash (a
/// partitioned operator: its partition key's). Re-shard asks, so the files it rewrites keep that
/// order and the restore after it can stay lazy (docs/design-durable-state.md §1f, increment 7).
/// </summary>
/// <summary>An operator that may checkpoint as its window store's own files
/// (docs/design-bounded-memory.md §3g) rather than as rows: a re-shard reads those back as rows.</summary>
internal interface IStoreSnapshot
{
    /// <summary>The rows a snapshot's store files hold, read into a scratch store (the operator is
    /// untouched); null when the snapshot holds none.</summary>
    ValueTask<ZSet<StructuralRow, Z64>?> ReadStoreRowsAsync(Circuit.ISnapshotReader reader, CancellationToken cancellationToken);
}

internal interface ISnapshotOrder
{
    /// <summary>The order hash for <paramref name="fileName"/> — a <c>Func&lt;TRow, int&gt;</c> over the
    /// operator's saved row type — or null for the row's own hash.</summary>
    object? OrderFor(string fileName);
}

/// <summary>Tags a freshly built snapshot Z-set (one with no trace behind it, as a re-shard writes)
/// with the order its runs must be written in.</summary>
internal sealed class SnapshotOrderTag<TKey, TWeight>(Func<TKey, int> order) : IZSetCheckpointTracked<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    public CheckpointTracking<ZSet<TKey, TWeight>>? Tracking => null;

    public Func<TKey, int>? OrderHash => order;

    public void StartTracking(object chain)
    {
    }

    /// <summary>Tag <paramref name="snapshot"/>.</summary>
    public static void Apply(ZSet<TKey, TWeight> snapshot, Func<TKey, int> order)
    {
        CheckpointTracking.RefuseShared(snapshot, ZSet<TKey, TWeight>.Empty);
        CheckpointTracking.Register(snapshot, new SnapshotOrderTag<TKey, TWeight>(order));
    }
}

/// <summary>
/// The cold partitions of a lazily restored partitioned operator: each partition's rows are read
/// the first time the operator touches the partition, and never again. Partition keys are hashed
/// with their own <c>GetHashCode</c> — the same function the save ordered the rows by
/// (<see cref="PartitionOrder"/>) — so this is used only where that hash is stable across processes.
/// </summary>
internal sealed class LazyPartitions<TKey, TRow>(IColdRows<TRow> cold, Func<TRow, TKey> partitionOf, IEqualityComparer<TKey> comparer)
    where TKey : notnull
    where TRow : notnull
{
    private readonly HashSet<TKey> _resolved = new(comparer);

    /// <summary>Rows still cold (an upper bound once partitions have been taken).</summary>
    public long ColdCount => cold.Count;

    /// <summary>Partitions read so far.</summary>
    public int Resolved => _resolved.Count;

    /// <summary><paramref name="key"/>'s rows if it has not been read yet; null if it has.</summary>
    public List<(TRow Row, long Weight)>? Take(TKey key)
    {
        if (!_resolved.Add(key))
        {
            return null;
        }

        var rows = new List<(TRow, long)>();
        foreach (var (row, weight) in cold.ReadByHash(key.GetHashCode()))
        {
            if (weight.Value != 0 && comparer.Equals(partitionOf(row), key))
            {
                rows.Add((row, weight.Value));
            }
        }

        return rows;
    }

    /// <summary>Every row of every partition not read yet; afterwards nothing is cold.</summary>
    public List<(TKey Key, TRow Row, long Weight)> TakeAll()
    {
        var rows = new List<(TKey, TRow, long)>();
        foreach (var (row, weight) in cold.ReadAll())
        {
            var key = partitionOf(row);
            if (weight.Value != 0 && !_resolved.Contains(key))
            {
                rows.Add((key, row, weight.Value));
            }
        }

        return rows;
    }

    /// <summary>The hash a partitioned operator's rows are saved in order of: their partition key's,
    /// when that is stable across processes (a <see cref="StructuralRow"/> key's cached hash);
    /// otherwise null, and the operator saves and restores eagerly as before.</summary>
    public static Func<TRow, int>? PartitionOrder(Func<TRow, TKey> partitionOf) =>
        typeof(TKey) == typeof(StructuralRow) ? row => partitionOf(row).GetHashCode() : null;
}
