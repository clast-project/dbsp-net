// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;
using DbspNet.Core.Circuit;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// What the two compiled shapes — <see cref="CompiledQuery"/> (one query, one output)
/// and <see cref="CompiledProgram"/> (a DAG of views in one circuit) — have in common:
/// a circuit, an input handle per source table, and a way to commit a tick.
/// </summary>
/// <remarks>
/// <para>This exists so machinery that only needs those three members works over either
/// shape. The persistence layer is the motivating consumer:
/// <c>DbspNet.Persistence.WalRecorder</c> captures per-tick input deltas
/// (<see cref="Inputs"/>), replays them (<see cref="Step"/>), and pairs the log with a
/// state snapshot of the <see cref="Circuit"/> — none of which is specific to a single
/// query. See <c>docs/design-incremental-persistence.md</c> §2.</para>
/// <para>Both implementations expose these members as their own public surface already,
/// so implementing this interface adds no members and changes no behaviour.</para>
/// </remarks>
public interface ICompiledCircuit
{
    /// <summary>The circuit holding every operator's state — the unit a snapshot covers.</summary>
    RootCircuit Circuit { get; }

    /// <summary>Input handle per source table, keyed by table name.</summary>
    IReadOnlyDictionary<string, TableInput> Inputs { get; }

    /// <summary>Commit the queued input deltas and fire the circuit one tick.</summary>
    void Step();
}
