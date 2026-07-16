// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Core.Circuit;

/// <summary>
/// Handle exposing the full materialized contents of a circuit output — the
/// running integral of its delta stream, maintained by an <c>IntegrateOp</c>
/// (the DBSP integration operator at the output boundary). After
/// <see cref="RootCircuit.Step"/>, <see cref="Current"/> is the whole current view,
/// for a truncate-mode sink to write as a full snapshot per batch. Distinct from
/// <see cref="OutputHandle{T}"/>, which exposes only the tick's delta.
/// </summary>
/// <remarks><see cref="Current"/> is a live reference whose contents change on the
/// next <see cref="RootCircuit.Step"/> — read it before stepping again.</remarks>
public sealed class IntegratedViewHandle<TRow>
    where TRow : notnull
{
    private readonly IntegrateOp<TRow> _op;

    internal IntegratedViewHandle(IntegrateOp<TRow> op)
    {
        _op = op;
    }

    /// <summary>The full view contents after the most recent step.</summary>
    public ZSet<TRow, Z64> Current => _op.View;
}
