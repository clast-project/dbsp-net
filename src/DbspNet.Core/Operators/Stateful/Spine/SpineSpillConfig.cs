// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.IO;

namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Configuration for disk spill on a <see cref="SpineZSetTrace{TKey,TWeight}"/>.
/// When supplied, batches that compact into a level at or below
/// <see cref="MinSpillLevel"/> stay resident in memory; batches that
/// land deeper get serialised to <see cref="FileSystem"/> via
/// <see cref="Codec"/> and their in-memory data is dropped (the
/// per-batch bloom stays so most probes skip the disk entirely).
/// </summary>
/// <remarks>
/// <para>Probe path for a spilled batch: bloom check (no disk); if
/// positive, the codec reloads the batch's data, the binary search
/// runs, and the loaded data is discarded. With a 1% bloom FPP only
/// ~1% of probes reach the disk per batch — fine when "trace doesn't
/// fit in RAM" is the binding constraint.</para>
/// <para>Compaction loads each input batch transiently, runs the
/// sorted merge, writes the result (which may itself spill if the new
/// level qualifies), and deletes the input files.</para>
/// </remarks>
public sealed class SpineSpillConfig<TKey, TWeight>
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    /// <summary>Filesystem to which spilled batches are written.</summary>
    public required ITableFileSystem FileSystem { get; init; }

    /// <summary>
    /// Key prefix under which spill files are created. The trace
    /// appends per-batch filenames to this prefix. Distinct prefixes
    /// per trace prevent collisions when multiple traces share a
    /// filesystem.
    /// </summary>
    public required string Prefix { get; init; }

    /// <summary>Codec used to serialise / deserialise each spilled batch.</summary>
    public required IZSetTraceCodec<TKey, TWeight> Codec { get; init; }

    /// <summary>
    /// Lowest level at which batches spill. Levels [0, MinSpillLevel)
    /// stay fully resident; levels [MinSpillLevel, ∞) spill on
    /// production. Default 2 keeps the small / hot L0 and L1 batches
    /// in memory while spilling the larger / colder deeper levels.
    /// </summary>
    public int MinSpillLevel { get; init; } = 2;
}

/// <summary>
/// Indexed-trace counterpart of <see cref="SpineSpillConfig{TKey,TWeight}"/>.
/// </summary>
public sealed class SpineIndexedSpillConfig<TKey, TValue, TWeight>
    where TKey : notnull
    where TValue : notnull
    where TWeight : struct, IZRing<TWeight>
{
    public required ITableFileSystem FileSystem { get; init; }

    public required string Prefix { get; init; }

    public required IIndexedZSetTraceCodec<TKey, TValue, TWeight> Codec { get; init; }

    public int MinSpillLevel { get; init; } = 2;
}
