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
