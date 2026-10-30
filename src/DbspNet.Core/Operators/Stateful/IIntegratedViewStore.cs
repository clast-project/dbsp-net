// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Alternative state for <see cref="IntegrateOp{TRow}"/>: something that holds the running
/// integral of a Z-set stream without keeping a <see cref="ZSet{TKey,TWeight}"/> of row objects.
/// The SQL layer's paged store (rows as values in reference-free pages,
/// docs/design-paged-trace-state.md) is the one implementation; without it the operator keeps
/// its <see cref="ZSetTrace{TKey,TWeight}"/>.
/// </summary>
internal interface IIntegratedViewStore<TRow>
    where TRow : notnull
{
    /// <summary>The number of rows with a non-zero weight.</summary>
    int Count { get; }

    void Integrate(ZSet<TRow, Z64> delta);

    Z64 WeightOf(TRow row);

    /// <summary>A fresh Z-set of the contents, enumerating in the order the equivalent
    /// in-place <see cref="ZSet{TKey,TWeight}"/> would.</summary>
    ZSet<TRow, Z64> Materialize();

    /// <summary>Retract every row, as <c>MergeInPlace(Negate())</c> does.</summary>
    void Clear();
}
