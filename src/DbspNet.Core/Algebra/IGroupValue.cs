// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Algebra;

/// <summary>
/// An (abelian) group: a monoid with an additive inverse. For every <c>x</c>,
/// <c>Add(x, Negate(x)) == Zero</c>. DBSP weights live in at least this
/// algebraic structure.
/// </summary>
public interface IGroupValue<TSelf> : IMonoidValue<TSelf>
    where TSelf : IGroupValue<TSelf>
{
    static abstract TSelf Negate(TSelf a);

    static virtual TSelf Subtract(TSelf a, TSelf b) => TSelf.Add(a, TSelf.Negate(b));
}
