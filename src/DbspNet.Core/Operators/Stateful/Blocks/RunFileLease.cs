// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Who still reads a spilled run file (docs/design-bounded-memory.md §5.20), so the file is deleted
/// as soon as nobody does, not when a garbage collection happens to find its run object dead.
/// <list type="bullet">
/// <item>A trace that lists a run over the file holds the lease and releases it the moment the run
/// leaves its levels (merged away, rebound to a checkpoint's copy, or the trace cleared).</item>
/// <item>A reader that captured runs beyond a call (a snapshot iterator, a block-backed delta's own
/// trace) holds it too but never releases: its hold ends when the reader is collected. Readers are
/// short-lived objects, so that is usually a young-generation collection.</item>
/// </list>
/// Holds are weak references, so a holder dropped without a release (a whole trace discarded) still
/// frees the file at the next <see cref="TryDelete"/>.
/// </summary>
internal sealed class RunFileLease
{
    private readonly List<WeakReference<object>> _holders = new(2);
    private Action? _delete;
    private int _pruneAt = 8;

    /// <param name="delete">Deletes the file; called once, on the thread whose release or sweep
    /// found no holder alive.</param>
    internal RunFileLease(Action delete) => _delete = delete;

    /// <summary>The file's length in bytes (the census).</summary>
    internal long Bytes { get; init; }

    /// <summary>The holders still alive, for the census (§5.20): what kind of reader keeps the file.</summary>
    internal List<object> AliveHolders()
    {
        var alive = new List<object>();
        lock (_holders)
        {
            foreach (var w in _holders)
            {
                if (w.TryGetTarget(out var h))
                {
                    alive.Add(h);
                }
            }
        }

        return alive;
    }

    /// <summary>Whether the file has been deleted.</summary>
    internal bool Deleted
    {
        get
        {
            lock (_holders)
            {
                return _delete is null;
            }
        }
    }

    /// <summary>Add <paramref name="holder"/> as a reader of the file.</summary>
    /// <exception cref="InvalidOperationException">The file is already deleted: a run over it was
    /// kept by something that never held it.</exception>
    internal void Hold(object holder)
    {
        ArgumentNullException.ThrowIfNull(holder);
        lock (_holders)
        {
            if (_delete is null)
            {
                throw new InvalidOperationException("a spilled run file was read after its last holder let it go");
            }

            if (_holders.Count >= _pruneAt)
            {
                PruneDead();
                _pruneAt = Math.Max(8, 2 * _holders.Count);
            }

            _holders.Add(new WeakReference<object>(holder));
        }
    }

    /// <summary><paramref name="holder"/> no longer reads the file (one hold of it); deletes the file
    /// if no other holder is alive.</summary>
    internal void Release(object holder)
    {
        lock (_holders)
        {
            for (var i = 0; i < _holders.Count; i++)
            {
                if (_holders[i].TryGetTarget(out var h) && ReferenceEquals(h, holder))
                {
                    _holders.RemoveAt(i);
                    break;
                }
            }
        }

        TryDelete();
    }

    /// <summary>Delete the file if no holder is alive; true if this call deleted it.</summary>
    internal bool TryDelete()
    {
        Action? delete;
        lock (_holders)
        {
            PruneDead();
            if (_holders.Count > 0 || _delete is null)
            {
                return false;
            }

            delete = _delete;
            _delete = null;
        }

        delete();
        return true;
    }

    private void PruneDead() => _holders.RemoveAll(static w => !w.TryGetTarget(out _));
}
