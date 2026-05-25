// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Buffers.Binary;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Input-side lateness enforcement and frontier source. For a stream carrying
/// a monotone (event-time-like) column, it:
/// <list type="number">
///   <item>drops rows whose monotone value is strictly below the current
///         frontier (a contract violation — they would touch state already
///         collected downstream), counting them;</item>
///   <item>tracks the maximum monotone value seen and advances the supplied
///         <see cref="MutableFrontier"/> to <c>maxSeen − lateness</c>.</item>
/// </list>
/// The same <see cref="MutableFrontier"/> is wired into downstream stateful
/// operators' GC, so the drop here is what keeps the frontier safe: no row
/// below the frontier ever reaches a trace whose sub-frontier state has been
/// reclaimed.
/// </summary>
/// <remarks>
/// The drop threshold is the frontier as it stood at the <i>start</i> of the
/// tick (advanced by prior ticks); the frontier then advances using this tick's
/// max. So a single batch is admitted/dropped against the prior watermark, and
/// downstream GC for the same tick uses the freshly-advanced one.
/// <para>
/// <c>maxSeen</c> is persisted (<see cref="ISnapshotable"/>): on restore the
/// frontier is re-advanced immediately, so the late-drop stays consistent with
/// the GC'd downstream traces that were restored alongside it.
/// </para>
/// </remarks>
internal sealed class LatenessOperator<TRow> : IOperator, ISnapshotable
    where TRow : notnull
{
    private const string StateFile = "frontier.bin";

    private readonly Stream<ZSet<TRow, Z64>> _input;
    private readonly Stream<ZSet<TRow, Z64>> _output;
    private readonly Func<TRow, long> _monotone;
    private readonly long _lateness;
    private readonly MutableFrontier _frontier;
    private long _maxSeen;
    private bool _hasMax;
    private long _droppedCount;

    public LatenessOperator(
        Stream<ZSet<TRow, Z64>> input,
        Stream<ZSet<TRow, Z64>> output,
        Func<TRow, long> monotone,
        long lateness,
        MutableFrontier frontier)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(monotone);
        ArgumentNullException.ThrowIfNull(frontier);
        if (lateness < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(lateness), lateness, "lateness must be non-negative");
        }

        _input = input;
        _output = output;
        _monotone = monotone;
        _lateness = lateness;
        _frontier = frontier;
    }

    /// <summary>Total rows dropped as late so far. Exposed for tests / observability.</summary>
    internal long DroppedCount => _droppedCount;

    public string SchemaFingerprint => string.Empty; // schemaless scalar state

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _output.SetCurrent(ZSet<TRow, Z64>.Empty);
            return;
        }

        var threshold = _frontier.Value; // frontier as of prior ticks
        var localMax = long.MinValue;
        var sawAny = false;
        ZSet<TRow, Z64> filtered;

        if (threshold == long.MinValue)
        {
            // No frontier advertised yet — admit everything.
            filtered = delta;
            foreach (var (row, _) in delta)
            {
                var t = _monotone(row);
                if (!sawAny || t > localMax)
                {
                    localMax = t;
                    sawAny = true;
                }
            }
        }
        else
        {
            var b = new ZSetBuilder<TRow, Z64>();
            foreach (var (row, w) in delta)
            {
                var t = _monotone(row);
                if (t < threshold)
                {
                    _droppedCount++;
                    continue;
                }

                b.Add(row, w);
                if (!sawAny || t > localMax)
                {
                    localMax = t;
                    sawAny = true;
                }
            }

            filtered = b.Build();
        }

        _output.SetCurrent(filtered);

        if (sawAny && (!_hasMax || localMax > _maxSeen))
        {
            _maxSeen = localMax;
            _hasMax = true;
        }

        if (_hasMax)
        {
            _frontier.AdvanceTo(SaturatingSub(_maxSeen, _lateness));
        }
    }

    public async ValueTask SaveAsync(ISnapshotWriter writer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        var buffer = new byte[9];
        buffer[0] = _hasMax ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(1), _maxSeen);

        await using var file = await writer.CreateAsync(StateFile, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        await stream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask LoadAsync(ISnapshotReader reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        if (!await reader.ExistsAsync(StateFile, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        await using var file = await reader.OpenReadAsync(StateFile, cancellationToken).ConfigureAwait(false);
        await using var stream = file.AsStream();
        var buffer = new byte[9];
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read), cancellationToken).ConfigureAwait(false);
            if (n == 0)
            {
                break;
            }

            read += n;
        }

        _hasMax = buffer[0] != 0;
        _maxSeen = BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(1));
        if (_hasMax)
        {
            // Re-advance the frontier so the late-drop resumes consistently
            // with the GC'd downstream state restored alongside this op.
            _frontier.AdvanceTo(SaturatingSub(_maxSeen, _lateness));
        }
    }

    private static long SaturatingSub(long a, long b) =>
        a < long.MinValue + b ? long.MinValue : a - b;
}
