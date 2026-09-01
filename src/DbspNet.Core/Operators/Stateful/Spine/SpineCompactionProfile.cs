// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Spine;

/// <summary>
/// Opt-in accounting for what compaction costs <b>on the step thread</b>.
/// </summary>
/// <remarks>
/// <para>`comparison-feldera-decisions.md` §9 row 3 argues that the flat-vs-spine measurement in
/// `decision-trace-family.md` was unfair — it compared LSM-with-in-step-compaction against a
/// dictionary, while Feldera merges on background threads — and that moving compaction off the step
/// thread would make it the experiment we thought we were running. That is only worth building if
/// merging is a meaningful share of the spine step in the first place, which nothing had measured.
/// This counts it.</para>
/// <para>Gated on <c>DBSPNET_SPINE_COMPACTION_PROFILE=1</c>, read once into a
/// <see langword="static"/> <see langword="readonly"/> field, so with the variable unset the JIT
/// folds every call site away and the merge path keeps its exact shipping shape.</para>
/// </remarks>
internal static class SpineCompactionProfile
{
    internal static readonly bool Enabled =
        Environment.GetEnvironmentVariable("DBSPNET_SPINE_COMPACTION_PROFILE") is "1" or "true" or "TRUE";

    private static readonly object Gate = new();

    /// <summary>Merges applied (one per <see cref="CompactionAction"/>).</summary>
    public static long Merges { get; private set; }

    /// <summary>Batches consumed by those merges.</summary>
    public static long BatchesMerged { get; private set; }

    /// <summary>Entries written by those merges — the work a background merger would move.</summary>
    public static long EntriesMerged { get; private set; }

    /// <summary>Wall time inside the merge itself.</summary>
    public static double MergeMs { get; private set; }

    /// <summary>Wall time building a batch from a delta or memtable flush — the other half of what
    /// <c>Integrate</c> does, and the part a background merger would <em>not</em> move.</summary>
    public static double BuildMs { get; private set; }

    public static long Builds { get; private set; }

    public static void AddMerge(double ms, int batches, int entries)
    {
        lock (Gate)
        {
            Merges++;
            BatchesMerged += batches;
            EntriesMerged += entries;
            MergeMs += ms;
        }
    }

    public static void AddBuild(double ms)
    {
        lock (Gate)
        {
            Builds++;
            BuildMs += ms;
        }
    }

    public static void Reset()
    {
        lock (Gate)
        {
            Merges = BatchesMerged = EntriesMerged = Builds = 0;
            MergeMs = BuildMs = 0;
        }
    }

    public static double MsSince(long from) =>
        (System.Diagnostics.Stopwatch.GetTimestamp() - from) * 1000.0
        / System.Diagnostics.Stopwatch.Frequency;
}
