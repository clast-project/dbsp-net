// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.IO;

/// <summary>
/// Optional capability of an <see cref="ITableFileSystem"/> whose writes are not durable on return
/// (a local filesystem: data sits in the OS page cache until flushed). Persistence code that must
/// survive power loss — the snapshot commit — checks for it and makes its writes durable explicitly.
/// </summary>
/// <remarks>
/// A backend whose every write is durable when it returns (an object store's single-blob PUT) does
/// not need to implement this; callers treat its plain writes as already durable.
/// </remarks>
public interface IDurableTableFileSystem : ITableFileSystem
{
    /// <summary>
    /// Make the contents of every file in <paramref name="paths"/>, and the directory entries that
    /// name them, durable. Returns only once all of it would survive power loss.
    /// </summary>
    ValueTask SyncAsync(IReadOnlyCollection<string> paths, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically replace <paramref name="path"/> with <paramref name="data"/>, durably: a reader —
    /// or a restart after power loss — sees either the previous contents or the new ones, never a
    /// missing or partial file. Used for commit pointers.
    /// </summary>
    ValueTask WriteAllBytesDurableAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
}
