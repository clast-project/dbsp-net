// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Compiler;

/// <summary>
/// Extracts the Int64 ordering key from a monotone column value, for the
/// LATENESS frontier. Temporal types are integers under the hood
/// (<c>Timestamp</c>/<c>Time64</c> microseconds, <c>Date32</c> days), and
/// logical-time columns are integers directly — so a single frontier
/// representation serves every carrier.
/// </summary>
internal static class MonotoneKey
{
    /// <summary>
    /// The Int64 ordering key of <paramref name="value"/>. The value comes from
    /// a column the resolver has already constrained to be NOT NULL and of an
    /// integer or temporal type, so a null or unexpected type is a compiler bug.
    /// </summary>
    public static long Extract(object? value) => value switch
    {
        long l => l,
        int i => i,
        Timestamp ts => ts.Microseconds,
        Time64 t => t.Microseconds,
        Date32 d => d.Days,
        _ => throw new System.InvalidOperationException(
            $"LATENESS column value is not a monotone Int64 carrier: {value?.GetType().Name ?? "null"}"),
    };
}
