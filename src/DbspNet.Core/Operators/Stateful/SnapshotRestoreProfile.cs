// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Opt-in <b>stage</b> apportionment of snapshot restore
/// (docs/design-incremental-persistence.md §11). §10 apportioned restore across operator
/// <em>kinds</em> and found it deserialize-bound; this splits the deserialize term itself into
/// the four things it is made of, so "lazy restore" can be priced instead of asserted:
/// <list type="bullet">
/// <item><b>read</b> — pulling the operator's file off disk into memory.</item>
/// <item><b>decode</b> — Arrow IPC framing to a <c>RecordBatch</c> (near zero-copy).</item>
/// <item><b>extract</b> — Arrow columns to <c>object?[]</c>, i.e. the boxing pass.</item>
/// <item><b>materialize</b> — one row object per row.</item>
/// <item><b>index</b> — hashing those rows into the loaded Z-set's dictionary.</item>
/// </list>
/// plus the two post-codec legs an operator pays: <b>integrate</b> (folding the loaded Z-set
/// into the operator's trace — a second hash of every key) and <b>rebuild</b> (re-partition /
/// re-sort / recompute for operators whose runtime state is not itself a Z-set).
/// </summary>
/// <remarks>
/// <para>Written only while <see cref="Enabled"/> is set, which
/// <c>Snapshot.ReadAsync</c> does for the duration of a restore when
/// <c>Snapshot.ProfileLoad</c> is on. Off by default and free when off; restore is
/// single-threaded, so plain non-atomic accumulation is exact.</para>
/// <para>The profiled codec path reads the whole file into memory before decoding, where the
/// unprofiled path decodes straight off the file stream. That is what makes the read leg
/// separable at all; it is a small distortion of the total, not of the split.</para>
/// </remarks>
internal static class SnapshotRestoreProfile
{
    /// <summary>Set for the duration of a profiled restore. Not thread-static: a restore runs
    /// on one flow, and the probe reads the totals after it returns.</summary>
    internal static bool Enabled;

    public static double ReadMs { get; private set; }

    public static double DecodeMs { get; private set; }

    public static double ExtractMs { get; private set; }

    /// <summary>The part of <see cref="ExtractMs"/> spent on VARCHAR columns, which today
    /// decode UTF-8 to a .NET string and re-encode it to UTF-8 (<c>Utf8String.Of(a.GetString(i))</c>)
    /// — the zero-copy alias path exists but the snapshot codecs do not use it.</summary>
    public static double ExtractStringMs { get; private set; }

    public static double MaterializeMs { get; private set; }

    public static double IndexMs { get; private set; }

    public static double IntegrateMs { get; private set; }

    public static double RebuildMs { get; private set; }

    public static long Files { get; private set; }

    public static long Rows { get; private set; }

    public static long Bytes { get; private set; }

    public static long Columns { get; private set; }

    /// <summary>VARCHAR columns among <see cref="Columns"/>.</summary>
    public static long StringColumns { get; private set; }

    public static void AddCodec(
        double readMs, double decodeMs, double extractMs, double materializeMs, double indexMs,
        long bytes, long rows, long columns, double extractStringMs = 0, long stringColumns = 0)
    {
        ExtractStringMs += extractStringMs;
        StringColumns += stringColumns;
        ReadMs += readMs;
        DecodeMs += decodeMs;
        ExtractMs += extractMs;
        MaterializeMs += materializeMs;
        IndexMs += indexMs;
        Bytes += bytes;
        Rows += rows;
        Columns += columns;
        Files++;
    }

    public static void AddIntegrate(double ms) => IntegrateMs += ms;

    public static void AddRebuild(double ms) => RebuildMs += ms;

    public static void Reset()
    {
        ReadMs = DecodeMs = ExtractMs = MaterializeMs = IndexMs = IntegrateMs = RebuildMs = 0;
        ExtractStringMs = 0;
        Files = Rows = Bytes = Columns = StringColumns = 0;
    }

    /// <summary>The stage totals in report order.</summary>
    public static IReadOnlyList<(string Name, double Ms)> Legs() => new[]
    {
        ("read (file I/O)", ReadMs),
        ("decode (Arrow IPC)", DecodeMs),
        ("extract (box cols)", ExtractMs),
        ("materialize (rows)", MaterializeMs),
        ("index (hash->ZSet)", IndexMs),
        ("integrate (->trace)", IntegrateMs),
        ("rebuild (op state)", RebuildMs),
    };

    /// <summary>Milliseconds since <paramref name="from"/>, a <c>Stopwatch.GetTimestamp()</c> value.</summary>
    public static double MsSince(long from) =>
        (System.Diagnostics.Stopwatch.GetTimestamp() - from) * 1000.0
        / System.Diagnostics.Stopwatch.Frequency;

    /// <summary>Milliseconds between two <c>Stopwatch.GetTimestamp()</c> values.</summary>
    public static double Ms(long from, long to) =>
        (to - from) * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
}
