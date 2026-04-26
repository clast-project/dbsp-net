namespace DbspNet.Core.Algebra;

/// <summary>
/// A monoid: a type with an associative binary <c>Add</c> and an identity
/// <c>Zero</c>. The identity satisfies <c>Add(Zero, x) == Add(x, Zero) == x</c>
/// for all <c>x</c>.
/// </summary>
public interface IMonoidValue<TSelf>
    where TSelf : IMonoidValue<TSelf>
{
    static abstract TSelf Zero { get; }

    static abstract TSelf Add(TSelf a, TSelf b);
}
