// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Persistence.IO.Local;

/// <summary>
/// Open read handles to local files, kept across reads (docs/design-bounded-memory.md §3.6): a block
/// fault reads ~60 KB at an offset, and opening and closing the file around it cost ~10× the read.
/// A small LRU, reference-counted so a handle evicted while a read uses it closes only after it.
/// Every create, rename, write and delete through <see cref="LocalTableFileSystem"/> invalidates its
/// path, so a handle never outlives the file it was opened on.
/// </summary>
internal static class LocalReadHandles
{
    /// <summary>Handles kept open at most (<c>DBSPNET_READ_HANDLES</c>, default 512). A process
    /// under a lower descriptor limit still works: on "too many open files" the idle handles close
    /// and the open is retried.</summary>
    public static int Capacity { get; } =
        int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_READ_HANDLES"), out var n) && n > 0 ? n : 512;

    internal sealed class Entry(string path, LocalRandomAccessFile file)
    {
        public readonly string Path = path;
        public readonly LocalRandomAccessFile File = file;
        public int Leases;
        public bool Retired;
        public LinkedListNode<Entry>? Node;
    }

    private static readonly object Gate = new();
    private static readonly Dictionary<string, Entry> Open = new(StringComparer.Ordinal);
    private static readonly LinkedList<Entry> Lru = new();

    /// <summary>A handle on <paramref name="fullPath"/> until disposed.</summary>
    public readonly struct Lease : IDisposable
    {
        private readonly Entry _entry;

        internal Lease(Entry entry) => _entry = entry;

        public LocalRandomAccessFile File => _entry.File;

        public void Dispose()
        {
            bool close;
            lock (Gate)
            {
                close = --_entry.Leases == 0 && _entry.Retired;
            }

            if (close)
            {
                _entry.File.Dispose();
            }
        }
    }

    public static Lease Rent(string fullPath)
    {
        Entry? closeNow = null;
        Lease lease;
        lock (Gate)
        {
            if (Open.TryGetValue(fullPath, out var entry))
            {
                Lru.Remove(entry.Node!);
                Lru.AddFirst(entry.Node!);
            }
            else
            {
                entry = new Entry(fullPath, OpenOrShed(fullPath));
                entry.Node = Lru.AddFirst(entry);
                Open[fullPath] = entry;
                if (Open.Count > Capacity)
                {
                    closeNow = Retire(Lru.Last!.Value);
                }
            }

            entry.Leases++;
            lease = new Lease(entry);
        }

        closeNow?.File.Dispose();
        return lease;
    }

    // Under the gate. Out of descriptors: close every idle handle, then try once more.
    private static LocalRandomAccessFile OpenOrShed(string fullPath)
    {
        try
        {
            return new LocalRandomAccessFile(fullPath);
        }
        catch (IOException) when (Open.Count > 0)
        {
            foreach (var idle in Open.Values.Where(e => e.Leases == 0).ToList())
            {
                Retire(idle);
                idle.File.Dispose();
            }

            return new LocalRandomAccessFile(fullPath);
        }
    }

    /// <summary>Forget <paramref name="fullPath"/>'s handle (the file is changing or going).</summary>
    public static void Invalidate(string fullPath)
    {
        Entry? closeNow = null;
        lock (Gate)
        {
            if (Open.TryGetValue(fullPath, out var entry))
            {
                closeNow = Retire(entry);
            }
        }

        closeNow?.File.Dispose();
    }

    /// <summary>Handles open now (tests).</summary>
    internal static int Count
    {
        get
        {
            lock (Gate)
            {
                return Open.Count;
            }
        }
    }

    // Under the gate: drop the entry; the caller closes what this returns (unleased) outside it.
    private static Entry? Retire(Entry entry)
    {
        Open.Remove(entry.Path);
        Lru.Remove(entry.Node!);
        entry.Retired = true;
        return entry.Leases == 0 ? entry : null;
    }
}
