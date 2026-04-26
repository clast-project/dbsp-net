using System.Numerics;
using CsCheck;

namespace DbspNet.Tests.Algebra;

// The prototype's Z-set weight is constrained as `struct, INumber<TWeight>`.
// These tests pin down the commutative-ring laws we rely on when reasoning
// about Z-set arithmetic. They run against `long` (the default weight) and
// double-check a second numeric type so we don't accidentally depend on
// long-specific behavior.

public sealed class NumericLawsTests
{
    // Weights are drawn from a bounded range so that checked arithmetic in
    // the ZSet layer doesn't overflow during property-based testing.
    private static readonly Gen<long> SmallLong = Gen.Int[-1000, 1000].Select(i => (long)i);

    [Fact]
    public void Long_Add_IsAssociative()
        => AddAssociative<long>(SmallLong);

    [Fact]
    public void Long_Add_IsCommutative()
        => AddCommutative<long>(SmallLong);

    [Fact]
    public void Long_Zero_IsAdditiveIdentity()
        => ZeroIsAdditiveIdentity<long>(SmallLong);

    [Fact]
    public void Long_Negation_IsAdditiveInverse()
        => NegationIsAdditiveInverse<long>(SmallLong);

    [Fact]
    public void Long_Mul_IsAssociative()
        => MulAssociative<long>(SmallLong);

    [Fact]
    public void Long_One_IsMultiplicativeIdentity()
        => OneIsMultiplicativeIdentity<long>(SmallLong);

    [Fact]
    public void Long_Mul_DistributesOverAdd()
        => MulDistributesOverAdd<long>(SmallLong);

    [Fact]
    public void Int_Add_IsAssociative()
        => AddAssociative<int>(Gen.Int[-1000, 1000]);

    [Fact]
    public void Int_Negation_IsAdditiveInverse()
        => NegationIsAdditiveInverse<int>(Gen.Int[-1000, 1000]);

    // ------- generic law bodies -------

    private static void AddAssociative<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Select(gen, gen).Sample((a, b, c) => (a + b) + c == a + (b + c));

    private static void AddCommutative<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Select(gen).Sample((a, b) => a + b == b + a);

    private static void ZeroIsAdditiveIdentity<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Sample(a => a + T.Zero == a && T.Zero + a == a);

    private static void NegationIsAdditiveInverse<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Sample(a => a + (-a) == T.Zero);

    private static void MulAssociative<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Select(gen, gen).Sample((a, b, c) => (a * b) * c == a * (b * c));

    private static void OneIsMultiplicativeIdentity<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Sample(a => a * T.One == a && T.One * a == a);

    private static void MulDistributesOverAdd<T>(Gen<T> gen) where T : struct, INumber<T>
        => gen.Select(gen, gen).Sample((a, b, c) => a * (b + c) == a * b + a * c);
}
