// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.IO;
using DbspNet.Persistence.IO;

namespace DbspNet.Tests.Persistence;

/// <summary>Thrown by <see cref="CrashSimulatingFileSystem"/> at the injected crash point.</summary>
internal sealed class SimulatedCrashException : Exception
{
    public SimulatedCrashException()
        : base("simulated crash")
    {
    }
}

/// <summary>
/// A store that can die at any chosen mutation and then be inspected as a restart would find it.
/// </summary>
/// <remarks>
/// <para>It keeps two images. The <b>live</b> image is what the process sees, and what survives a
/// process crash (the OS still has every write). The <b>durable</b> image holds only what an
/// explicit <see cref="SyncAsync"/> or <see cref="WriteAllBytesDurableAsync"/> made durable — the
/// files a restart after <b>power loss</b> finds. Creates, writes, renames and deletes are all
/// volatile until synced, which is the POSIX contract the local store has to live with.</para>
/// <para>Every mutation (create, each sequential write, rename, delete, small-file write, sync)
/// is counted; when the count reaches <see cref="CrashAt"/> that mutation and every later one
/// throws <see cref="SimulatedCrashException"/>, so a test can enumerate every crash point of a
/// commit by setting <c>CrashAt = Mutations + k</c> for k = 0, 1, ….</para>
/// </remarks>
internal sealed class CrashSimulatingFileSystem : IDurableTableFileSystem
{
    private readonly InMemoryTableFileSystem _live;
    private readonly Dictionary<string, byte[]> _durable;
    private bool _crashed;

    public CrashSimulatingFileSystem()
        : this(new InMemoryTableFileSystem(), new Dictionary<string, byte[]>(StringComparer.Ordinal))
    {
    }

    private CrashSimulatingFileSystem(InMemoryTableFileSystem live, Dictionary<string, byte[]> durable)
    {
        _live = live;
        _durable = durable;
    }

    /// <summary>Mutations performed so far.</summary>
    public long Mutations { get; private set; }

    /// <summary>Index of the mutation that crashes; <see cref="long.MaxValue"/> = never.</summary>
    public long CrashAt { get; set; } = long.MaxValue;

    /// <summary>The store a restart sees after the process died: everything it wrote is there.</summary>
    public async Task<CrashSimulatingFileSystem> AfterProcessCrashAsync()
    {
        var live = new InMemoryTableFileSystem();
        await foreach (var entry in _live.ListAsync(string.Empty))
        {
            await live.WriteAllBytesAsync(entry.Path, await _live.ReadAllBytesAsync(entry.Path));
        }

        return new CrashSimulatingFileSystem(live, new Dictionary<string, byte[]>(_durable, StringComparer.Ordinal));
    }

    /// <summary>The store a restart sees after power loss: only what was made durable.</summary>
    public async Task<CrashSimulatingFileSystem> AfterPowerLossAsync()
    {
        var live = new InMemoryTableFileSystem();
        foreach (var (path, bytes) in _durable)
        {
            await live.WriteAllBytesAsync(path, bytes);
        }

        return new CrashSimulatingFileSystem(live, new Dictionary<string, byte[]>(_durable, StringComparer.Ordinal));
    }

    private void Mutate()
    {
        if (_crashed || Mutations == CrashAt)
        {
            _crashed = true;
            throw new SimulatedCrashException();
        }

        Mutations++;
    }

    public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
        _live.ListAsync(prefix, cancellationToken);

    public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default) =>
        _live.OpenReadAsync(path, cancellationToken);

    public async ValueTask<ISequentialFile> CreateAsync(
        string path, bool overwrite = false, CancellationToken cancellationToken = default)
    {
        Mutate();
        return new CountingFile(this, await _live.CreateAsync(path, overwrite, cancellationToken));
    }

    public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default)
    {
        Mutate();
        return _live.RenameAsync(sourcePath, targetPath, cancellationToken);
    }

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default)
    {
        Mutate();
        return _live.DeleteAsync(path, cancellationToken);
    }

    public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
        _live.ExistsAsync(path, cancellationToken);

    public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default) =>
        _live.ReadAllBytesAsync(path, cancellationToken);

    public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Mutate();
        return _live.WriteAllBytesAsync(path, data, cancellationToken);
    }

    public async ValueTask SyncAsync(IReadOnlyCollection<string> paths, CancellationToken cancellationToken = default)
    {
        Mutate();
        foreach (var path in paths)
        {
            if (await _live.ExistsAsync(path, cancellationToken))
            {
                _durable[path] = await _live.ReadAllBytesAsync(path, cancellationToken);
            }
            else
            {
                _durable.Remove(path);
            }
        }
    }

    public async ValueTask WriteAllBytesDurableAsync(
        string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Mutate();
        await _live.WriteAllBytesAsync(path, data, cancellationToken);
        _durable[path] = data.ToArray();
    }

    private sealed class CountingFile(CrashSimulatingFileSystem owner, ISequentialFile inner) : ISequentialFile
    {
        public long Position => inner.Position;

        public ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            owner.Mutate();
            return inner.WriteAsync(data, cancellationToken);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);

        public void Dispose() => inner.Dispose();

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
