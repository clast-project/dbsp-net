// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Where an entry of a stateful operator's persisted state lives in a data-parallel program,
/// stated by the compiler that built the operator (docs/design-durable-state.md §1d). Re-sharding a
/// snapshot to a different worker count sends each entry where this says.
/// </summary>
public abstract record ReshardPlacement
{
    private ReshardPlacement()
    {
    }

    /// <summary>On worker <c>hash % W</c>, with the hash computed from the persisted entry: the
    /// row of a Z-set trace, or the key of an indexed trace or keyed-blob file. Must be the same
    /// function the exchange in front of the operator uses.</summary>
    public sealed record Hash(Func<StructuralRow, int> Of) : ReshardPlacement;

    /// <summary>Gathered: all of it on worker 0.</summary>
    public sealed record Worker0 : ReshardPlacement;

    /// <summary>A full, identical copy on every worker (the broadcast side of a join).</summary>
    public sealed record Replicated : ReshardPlacement;

    /// <summary>Any placement: the operator is linear and every consumer reads the sum over
    /// workers, so where an entry sits does not change the result.</summary>
    public sealed record Linear : ReshardPlacement;

    public static ReshardPlacement OnWorker0 { get; } = new Worker0();

    public static ReshardPlacement OnEveryWorker { get; } = new Replicated();

    public static ReshardPlacement Anywhere { get; } = new Linear();

    /// <summary>On <c>OfWholeRow(entry, schema) % W</c>.</summary>
    public static ReshardPlacement WholeRow(Schema schema) =>
        new Hash(row => StablePartitionHash.OfWholeRow(row, schema));
}

/// <summary>One file a stateful operator writes into its snapshot directory, the codec it is
/// written with, and where its entries live.</summary>
public abstract record ReshardFile(string Name, ReshardPlacement Placement);

/// <summary>A Z-set trace file (<c>trace.arrows</c>, <c>view.arrows</c>); placement over the row.</summary>
public sealed record ZSetReshardFile(
    string Name, ReshardPlacement Placement, IZSetTraceCodec<StructuralRow, Z64> Codec)
    : ReshardFile(Name, Placement);

/// <summary>An indexed trace file (<c>trace.arrows</c>, <c>left.arrows</c>, <c>right.arrows</c>);
/// placement over the key.</summary>
public sealed record IndexedReshardFile(
    string Name, ReshardPlacement Placement, IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> Codec)
    : ReshardFile(Name, Placement);

/// <summary>A keyed-blob file (an aggregate's <c>aggstate.arrows</c>); placement over the key.
/// Optional: an operator may not write it.</summary>
public sealed record KeyedBlobsReshardFile(
    string Name, ReshardPlacement Placement, IIndexedZSetTraceCodec<StructuralRow, StructuralRow, Z64> Codec)
    : ReshardFile(Name, Placement);

/// <summary>Every file one stateful operator writes, or why it cannot be re-sharded.</summary>
public sealed record ReshardSpec(string OperatorType, IReadOnlyList<ReshardFile> Files, string? Unsupported = null);

/// <summary>
/// Collects <see cref="ReshardSpec"/>s while the parallel compile builds replica 0: each stateful
/// build site marks the operator count, builds, and records the files and placement of the one
/// snapshotable operator it added. Replicas are built from the same plan, so replica 0's operator
/// indices hold for all of them.
/// </summary>
internal sealed class ReshardRecorder
{
    private readonly Dictionary<int, ReshardSpec> _specs = [];

    /// <summary>True only while replica 0 is being built.</summary>
    public bool Active { get; set; }

    public IReadOnlyDictionary<int, ReshardSpec> Specs => _specs;

    public static int Mark(CircuitBuilder builder) => builder.OperatorCount;

    public void Record(CircuitBuilder builder, int mark, params ReshardFile?[] files)
    {
        if (!Active)
        {
            return;
        }

        var added = new List<int>();
        for (var i = mark; i < builder.OperatorCount; i++)
        {
            if (builder.OperatorAt(i) is ISnapshotable)
            {
                added.Add(i);
            }
        }

        if (added.Count != 1)
        {
            throw new InvalidOperationException(
                $"a stateful build site added {added.Count} snapshotable operators; reshard recording expects exactly one");
        }

        var type = builder.OperatorAt(added[0]).GetType().Name;
        var present = files.Where(f => f is not null).Cast<ReshardFile>().ToList();
        _specs[added[0]] = type.StartsWith("Spine", StringComparison.Ordinal)
            ? new ReshardSpec(type, present, "spine traces cannot be re-sharded yet (docs/design-durable-state.md §1d)")
            : new ReshardSpec(type, present);
    }
}
