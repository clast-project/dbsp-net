// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

internal interface IInputCommit
{
    void Commit();
}

/// <summary>
/// Handle used by callers to feed values into a circuit input stream. Values
/// pushed between ticks are merged using the <c>merge</c> function supplied
/// at creation; if no push happens before <see cref="RootCircuit.Step"/>,
/// the stream carries its <c>zero</c> for that tick.
/// </summary>
public sealed class InputHandle<T> : IInputCommit
{
    private readonly RootCircuit _root;
    private readonly Stream<T> _stream;
    private readonly Func<T, T, T> _merge;
    private readonly Func<IReadOnlyList<T>, T>? _mergeMany;
    private readonly T _zero;
    private T _pending;
    private bool _hasPending;

    // With a many-way merge: every value pushed this tick, merged once at commit. A
    // pairwise merge per push re-copies the accumulated value each time — quadratic in
    // the pushes a tick takes (a bulk load pushes a table as many record batches).
    private List<T>? _queued;

    internal InputHandle(
        RootCircuit root, Stream<T> stream, T zero, Func<T, T, T> merge, Func<IReadOnlyList<T>, T>? mergeMany = null)
    {
        _root = root;
        _stream = stream;
        _merge = merge;
        _mergeMany = mergeMany;
        _zero = zero;
        _pending = zero;
    }

    /// <summary>
    /// Queue a value to be delivered on the next <see cref="RootCircuit.Step"/>.
    /// Multiple pushes in the same tick are merged via the merge function
    /// supplied at construction (e.g. <c>Plus</c> for Z-set streams).
    /// </summary>
    public void Push(T value)
    {
        lock (_root.SyncRoot)
        {
            if (_mergeMany is not null)
            {
                (_queued ??= []).Add(value);
                return;
            }

            _pending = _hasPending ? _merge(_pending, value) : value;
            _hasPending = true;
        }
    }

    void IInputCommit.Commit()
    {
        if (_mergeMany is not null)
        {
            _stream.SetCurrent(_queued is not { Count: > 0 } queued
                ? _zero
                : queued.Count == 1 ? queued[0] : _mergeMany(queued));
            _queued?.Clear();
            return;
        }

        _stream.SetCurrent(_hasPending ? _pending : _zero);
        _pending = _zero;
        _hasPending = false;
    }
}
