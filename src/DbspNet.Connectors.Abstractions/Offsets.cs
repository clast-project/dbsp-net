// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// An opaque, comparable, serializable position in a source — the connector's notion
/// of progress. For a Delta source it wraps a version (<see cref="LongOffset"/>); a
/// Kafka source would wrap a partition→offset map. Persisted in the checkpoint so a
/// restart resumes exactly where the last durable tick left off. See
/// <c>docs/design-connectors.md</c>.
/// </summary>
public interface IConnectorOffset : IComparable<IConnectorOffset>
{
    /// <summary>The durable string form written into the checkpoint manifest.</summary>
    string Serialize();
}

/// <summary>
/// A monotone <see cref="long"/> offset — the common case (Delta version, file
/// sequence). <see cref="LongOffset.Before"/> is the "before any data" sentinel
/// (value <c>-1</c>, since Delta versions start at 0).
/// </summary>
public sealed record LongOffset(long Value) : IConnectorOffset
{
    /// <summary>The sentinel meaning "nothing consumed yet".</summary>
    public static LongOffset Before { get; } = new(-1);

    public string Serialize() => Value.ToString(CultureInfo.InvariantCulture);

    public static LongOffset Parse(string s) =>
        new(long.Parse(s, CultureInfo.InvariantCulture));

    public int CompareTo(IConnectorOffset? other) => other switch
    {
        null => 1,
        LongOffset o => Value.CompareTo(o.Value),
        _ => throw new ArgumentException(
            $"cannot compare LongOffset with {other.GetType().Name}", nameof(other)),
    };
}

/// <summary>One source's committed offset at a checkpoint (source name → serialized
/// offset). Persisted alongside the engine snapshot so engine-tick and source-offset
/// restore consistently.</summary>
public readonly record struct SourceCheckpoint(string SourceName, string Offset);
