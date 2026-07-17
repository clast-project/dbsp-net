// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Server;

/// <summary>
/// Deploy payload: the SQL program (a DAG of <c>CREATE TABLE</c> sources and
/// <c>CREATE VIEW</c> definitions, in dependency order), the Delta input bindings (one
/// per source table), and the Delta output bindings (one per output view). The analogue
/// of Feldera's pipeline program + <c>+connectors</c> config.
/// </summary>
public sealed record ProgramSpec(
    IReadOnlyList<string> Program,
    IReadOnlyList<InputSpec> Inputs,
    IReadOnlyList<OutputSpec> Outputs);

/// <summary>An input binding: a program source <paramref name="Table"/> fed from the
/// Delta table at <paramref name="Uri"/>. <paramref name="Mode"/> is accepted for parity
/// with Feldera (snapshot / snapshot_and_follow) but ignored — the connector follows the
/// change-data-feed from version 0, which subsumes both.</summary>
public sealed record InputSpec(string Table, string Uri, string? Mode = null);

/// <summary>An output binding: a program output <paramref name="View"/> written to the
/// Delta table at <paramref name="Uri"/> (truncate mode).</summary>
public sealed record OutputSpec(string View, string Uri, string? Mode = null);

/// <summary>Result of a deploy: the compile time (excluded from measured batch duration,
/// as Feldera excludes its Rust compile).</summary>
public sealed record DeployResult(double CompileTimeS, int InputCount, int OutputCount);

/// <summary>Result of resume: the epoch second the batch timer started.</summary>
public sealed record ResumeResult(long ResumedAtEpochS);

/// <summary>Result of wait: the measured batch duration, the tick count, and per-output
/// row counts (all marked success — the benchmark's per-node contract).</summary>
public sealed record WaitResult(double DurationS, long Ticks, IReadOnlyList<OutputStat> Outputs);

/// <summary>One output view's materialised row count after a batch.</summary>
public sealed record OutputStat(string View, long Rows, string Status = "success");

/// <summary>Point-in-time engine stats (for polling / drain observation).</summary>
public sealed record EngineStats(
    bool Deployed, bool BatchRunning, long TickCount, IReadOnlyList<OutputStat> Outputs);
