// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Circuit;

/// <summary>
/// A point-in-time observability snapshot of one stateful operator, produced by
/// <see cref="RootCircuit.CollectStats"/>. Useful for watching a long-running,
/// bounded-memory pipeline — e.g. seeing trace state shrink as the LATENESS /
/// clock frontier advances.
/// </summary>
/// <param name="Index">The operator's position in <see cref="RootCircuit"/>'s
/// registration order — the same stable id the persistence layer uses.</param>
/// <param name="Name">A short operator-kind label (e.g. <c>IncrementalAggregate</c>).</param>
/// <param name="RetainedRows">Rows / keys currently held in the operator's
/// state (the integral / trace). The headline bounded-memory number.</param>
/// <param name="LastOutputRows">Distinct rows in the delta the operator emitted
/// on the most recent tick (a churn indicator).</param>
/// <param name="GcFrontier">The current GC frontier the operator collects
/// against, or <c>null</c> if it has no frontier wired (no LATENESS / clock
/// watermark reaches it).</param>
/// <param name="GcDroppedTotal">Cumulative count of state rows / keys the
/// operator has garbage-collected since it was created.</param>
public readonly record struct OperatorStat(
    int Index,
    string Name,
    long RetainedRows,
    long LastOutputRows,
    long? GcFrontier,
    long GcDroppedTotal)
{
    public override string ToString()
    {
        var frontier = GcFrontier is { } f
            ? f.ToString(System.Globalization.CultureInfo.InvariantCulture)
            : "—";
        return System.FormattableString.Invariant(
            $"[{Index}] {Name}: state={RetainedRows} out={LastOutputRows} frontier={frontier} gc={GcDroppedTotal}");
    }
}

/// <summary>
/// Per-operator cumulative timing from <see cref="RootCircuit.CollectOperatorProfile"/>:
/// one operator's total wall-clock across every profiled <see cref="RootCircuit.Step"/>,
/// paired with a type label and (for stateful operators) its state / output sizes.
/// <paramref name="RetainedRows"/> and <paramref name="LastOutputRows"/> are <c>-1</c>
/// for stateless (linear) operators that expose no metrics.
/// </summary>
public readonly record struct OperatorProfile(
    int Index,
    string Name,
    double CumulativeMs,
    long RetainedRows,
    long LastOutputRows)
{
    public override string ToString() => System.FormattableString.Invariant(
        $"[{Index}] {Name}: {CumulativeMs:F1}ms state={RetainedRows} out={LastOutputRows}");
}

/// <summary>
/// Implemented by stateful operators that expose runtime metrics to
/// <see cref="RootCircuit.CollectStats"/>. Stateless (linear) operators do not
/// implement it and simply don't appear in the collected stats. Reads are
/// on-demand (never on the hot <see cref="RootCircuit.Step"/> path), so an
/// implementation may compute a value lazily even if doing so is O(state).
/// </summary>
/// <summary>Helpers for operators implementing <see cref="IIntrospectable"/>.</summary>
internal static class Metric
{
    /// <summary>An <see cref="IFrontier"/>'s value as a nullable for
    /// <see cref="OperatorStat.GcFrontier"/> — <c>null</c> when the frontier is
    /// absent or still unset (<see cref="long.MinValue"/>).</summary>
    public static long? Frontier(IFrontier? frontier) =>
        frontier is { } f && f.Value != long.MinValue ? f.Value : null;
}

internal interface IIntrospectable
{
    /// <summary>A short operator-kind label for <see cref="OperatorStat.Name"/>.</summary>
    string MetricName { get; }

    /// <summary>Rows / keys currently retained in this operator's state.</summary>
    long RetainedRows { get; }

    /// <summary>Distinct rows in the delta emitted on the most recent tick.</summary>
    long LastOutputRows { get; }

    /// <summary>The GC frontier this operator collects against, or <c>null</c>.</summary>
    long? GcFrontier { get; }

    /// <summary>Cumulative state rows / keys garbage-collected since creation.</summary>
    long GcDroppedTotal { get; }
}
