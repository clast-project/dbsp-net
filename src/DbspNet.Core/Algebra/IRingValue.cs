namespace DbspNet.Core.Algebra;

/// <summary>
/// A ring: an abelian group with an associative multiplication that is
/// distributive over addition, and a multiplicative identity <c>One</c>.
/// DBSP's bilinear operators (join, cartesian product) rely on this structure.
/// </summary>
public interface IRingValue<TSelf> : IGroupValue<TSelf>
    where TSelf : IRingValue<TSelf>
{
    static abstract TSelf One { get; }

    static abstract TSelf Multiply(TSelf a, TSelf b);
}
