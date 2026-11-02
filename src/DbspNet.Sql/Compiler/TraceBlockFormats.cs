// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Operators.Stateful.Blocks;
using DbspNet.Sql.Plan;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Where the compiler gets a block encoding for a trace (docs/design-flat-trace-state.md §3). The
/// encoding is the v3 run block, which needs Arrow, so it lives in DbspNet.Persistence; that
/// assembly installs <see cref="Factory"/> when it loads. Without it, or for a schema with a type
/// that has no block encoding, the trace stays managed.
/// </summary>
internal static class TraceBlockFormats
{
    /// <summary>The block format for a trace keyed by the first schema holding rows of the
    /// second, or <see langword="null"/>.</summary>
    internal static Func<Schema, Schema, ITraceBlockFormat?>? Factory { get; set; }

    internal static ITraceBlockFormat? Create(Schema keySchema, Schema valueSchema) => Factory?.Invoke(keySchema, valueSchema);
}
