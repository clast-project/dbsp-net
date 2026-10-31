// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using DbspNet.Core.IO;

namespace DbspNet.Persistence;

/// <summary>One engine tick in the journal: the source it consumed and that source's cursor
/// (serialized offset) after it.</summary>
public sealed record JournalTick(long Tick, string Source, string Offset);

/// <summary>
/// The per-batch commit of the A2 checkpoint policy (docs/design-durable-state.md §1c): which
/// source each engine tick consumed and where that source's cursor stood after it, so a restart
/// can restore the last snapshot and replay the ticks since it exactly.
/// </summary>
/// <remarks>
/// <para>One file per batch, <c>journal/{first:D20}-{last:D20}.json</c>, written atomically and
/// durably on an <see cref="IDurableTableFileSystem"/> (whole or absent after a crash), so there is
/// no torn tail to repair. Ticks across files must be contiguous; a gap fails the read.</para>
/// <para>Offsets only, never row data: every connector can re-read a version. Lives beside the
/// snapshots in the same store; <see cref="TruncateThroughAsync"/> drops what a committed snapshot
/// covers.</para>
/// </remarks>
public sealed class TickJournal
{
    internal const string Prefix = "journal/";
    private const int FormatVersion = 1;

    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    private readonly ITableFileSystem _fs;

    public TickJournal(ITableFileSystem fs) => _fs = fs ?? throw new ArgumentNullException(nameof(fs));

    /// <summary>Durably append one batch's ticks, which must be in order and contiguous.</summary>
    public async ValueTask AppendAsync(IReadOnlyList<JournalTick> ticks, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ticks);
        if (ticks.Count == 0)
        {
            return;
        }

        for (var i = 1; i < ticks.Count; i++)
        {
            if (ticks[i].Tick != ticks[i - 1].Tick + 1)
            {
                throw new ArgumentException("journal ticks must be contiguous", nameof(ticks));
            }
        }

        var key = Key(ticks[0].Tick, ticks[^1].Tick);
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new JournalFile(FormatVersion, ticks), Json);
        var durable = Snapshot.DurableStore(_fs);
        if (durable is not null)
        {
            await durable.WriteAllBytesDurableAsync(key, bytes, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _fs.WriteAllBytesAsync(key, bytes, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Every journaled tick after <paramref name="tick"/>, in order. Throws
    /// <see cref="InvalidDataException"/> if they do not continue from <paramref name="tick"/> + 1
    /// without a gap.</summary>
    public async ValueTask<IReadOnlyList<JournalTick>> ReadAfterAsync(long tick, CancellationToken cancellationToken = default)
    {
        var files = await ListAsync(cancellationToken).ConfigureAwait(false);
        var result = new List<JournalTick>();
        var next = tick + 1;
        foreach (var (first, last, key) in files)
        {
            if (last <= tick)
            {
                continue;
            }

            var bytes = await _fs.ReadAllBytesAsync(key, cancellationToken).ConfigureAwait(false);
            var file = JsonSerializer.Deserialize<JournalFile>(bytes, Json)
                ?? throw new InvalidDataException($"journal file {key} is empty");
            if (file.Version != FormatVersion)
            {
                throw new InvalidDataException($"journal file {key} has format version {file.Version}, expected {FormatVersion}");
            }

            foreach (var t in file.Ticks)
            {
                if (t.Tick <= tick)
                {
                    continue;
                }

                if (t.Tick != next)
                {
                    throw new InvalidDataException(
                        $"journal is missing tick {next} (next journaled tick is {t.Tick}, in {key})");
                }

                result.Add(t);
                next++;
            }

            if (first > last)
            {
                throw new InvalidDataException($"journal file {key} has a reversed tick range");
            }
        }

        return result;
    }

    /// <summary>Delete every journal file whose ticks are all at or below <paramref name="tick"/>
    /// — call only once a snapshot at <paramref name="tick"/> is durably committed. Best effort: a
    /// file left behind is ignored by <see cref="ReadAfterAsync"/> and removed next time.</summary>
    public async ValueTask TruncateThroughAsync(long tick, CancellationToken cancellationToken = default)
    {
        foreach (var (_, last, key) in await ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (last <= tick)
            {
                await _fs.DeleteAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static string Key(long first, long last) =>
        string.Create(CultureInfo.InvariantCulture, $"{Prefix}{first:D20}-{last:D20}.json");

    // Journal files in tick order. Anything else under the prefix (an interrupted atomic write's
    // temporary) is ignored.
    private async ValueTask<List<(long First, long Last, string Key)>> ListAsync(CancellationToken cancellationToken)
    {
        var files = new List<(long, long, string)>();
        await foreach (var f in _fs.ListAsync(Prefix, cancellationToken).ConfigureAwait(false))
        {
            var name = f.Path.StartsWith(Prefix, StringComparison.Ordinal) ? f.Path[Prefix.Length..] : f.Path;
            if (name.Length != 46 || !name.EndsWith(".json", StringComparison.Ordinal) || name[20] != '-'
                || !long.TryParse(name.AsSpan(0, 20), NumberStyles.None, CultureInfo.InvariantCulture, out var first)
                || !long.TryParse(name.AsSpan(21, 20), NumberStyles.None, CultureInfo.InvariantCulture, out var last))
            {
                continue;
            }

            files.Add((first, last, Prefix + name));
        }

        files.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return files;
    }

    private sealed record JournalFile(
        [property: JsonPropertyName("version")] int Version,
        [property: JsonPropertyName("ticks")] IReadOnlyList<JournalTick> Ticks);
}
