// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Collections.Generic;

namespace DbspNet.Server;

/// <summary>
/// Deploy payload: the SQL program (a DAG of <c>CREATE TABLE</c> sources and
/// <c>CREATE VIEW</c> definitions, in dependency order), the Delta input bindings (one
/// per source table), and the Delta output bindings (one per output view). The analogue
/// of Feldera's pipeline program + <c>+connectors</c> config.
/// <para>
/// <paramref name="SnapshotDir"/> (optional) turns on per-batch state persistence: the
/// program is compiled with snapshot codecs and each batch ends with a durable
/// checkpoint (engine state + source cursors) under that directory, restored on the
/// next deploy. Falls back to the <c>DBSPNET_SNAPSHOT_DIR</c> environment variable when
/// unset; persistence is off when neither is given (the codec-free compile, so the
/// engine is bit-for-bit what it was before this option existed).
/// </para>
/// </summary>
public sealed record ProgramSpec(
    IReadOnlyList<string> Program,
    IReadOnlyList<InputSpec> Inputs,
    IReadOnlyList<OutputSpec> Outputs,
    string? SnapshotDir = null);

/// <summary>An input binding: a program source <paramref name="Table"/> fed from the
/// Delta table at <paramref name="Uri"/>. <paramref name="Mode"/> is accepted for parity
/// with Feldera (snapshot / snapshot_and_follow) but ignored — the connector follows the
/// change-data-feed from version 0, which subsumes both.</summary>
public sealed record InputSpec(string Table, string Uri, string? Mode = null);

/// <summary>An output binding: a program output <paramref name="View"/> written to the
/// Delta table at <paramref name="Uri"/> (truncate mode).</summary>
public sealed record OutputSpec(string View, string Uri, string? Mode = null);

/// <summary>Result of a deploy: the compile time (excluded from measured batch duration,
/// as Feldera excludes its Rust compile), the binding counts, whether per-batch state
/// persistence is on, and the engine tick restored from a pre-existing checkpoint (0 =
/// fresh start, the normal benchmark case).</summary>
public sealed record DeployResult(
    double CompileTimeS,
    int InputCount,
    int OutputCount,
    bool Persistent = false,
    long RestoredTick = 0);

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

/// <summary>Dry-run compile check: the SQL program (CREATE TABLE + CREATE VIEW, in
/// dependency order) and the output view names. No connectors / no Delta needed.</summary>
public sealed record CompileSpec(IReadOnlyList<string> Program, IReadOnlyList<string> Outputs);

/// <summary>Result of a dry-run compile: whether the whole program compiled into one
/// circuit, the output-view count, and the first error if not.</summary>
public sealed record CompileResult(bool Ok, int OutputViews, string? Error);
