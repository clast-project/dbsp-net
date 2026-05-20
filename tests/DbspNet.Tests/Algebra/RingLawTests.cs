// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;

namespace DbspNet.Tests.Algebra;

public class RingLawTests
{
    // Restrict to a range that won't overflow under multiplication.
    private static readonly Gen<Z64> GenZ64 =
        Gen.Long[-1_000_000, 1_000_000].Select(v => new Z64(v));

    [Fact]
    public void Add_IsAssociative()
    {
        Gen.Select(GenZ64, GenZ64, GenZ64)
            .Sample((a, b, c) => Z64.Add(Z64.Add(a, b), c) == Z64.Add(a, Z64.Add(b, c)));
    }

    [Fact]
    public void Add_IsCommutative()
    {
        Gen.Select(GenZ64, GenZ64).Sample((a, b) => Z64.Add(a, b) == Z64.Add(b, a));
    }

    [Fact]
    public void Add_HasZeroIdentity()
    {
        GenZ64.Sample(a => Z64.Add(a, Z64.Zero) == a && Z64.Add(Z64.Zero, a) == a);
    }

    [Fact]
    public void Negate_IsAdditiveInverse()
    {
        GenZ64.Sample(a => Z64.Add(a, Z64.Negate(a)) == Z64.Zero);
    }

    [Fact]
    public void Multiply_IsAssociative()
    {
        // Narrow range further to stay below product overflow.
        var smaller = Gen.Long[-1000, 1000].Select(v => new Z64(v));
        Gen.Select(smaller, smaller, smaller)
            .Sample((a, b, c) => Z64.Multiply(Z64.Multiply(a, b), c) == Z64.Multiply(a, Z64.Multiply(b, c)));
    }

    [Fact]
    public void Multiply_HasOneIdentity()
    {
        GenZ64.Sample(a => Z64.Multiply(a, Z64.One) == a && Z64.Multiply(Z64.One, a) == a);
    }

    [Fact]
    public void Multiply_DistributesOverAdd()
    {
        var smaller = Gen.Long[-1000, 1000].Select(v => new Z64(v));
        Gen.Select(smaller, smaller, smaller).Sample((a, b, c) =>
            Z64.Multiply(a, Z64.Add(b, c)) == Z64.Add(Z64.Multiply(a, b), Z64.Multiply(a, c)));
    }

    [Fact]
    public void IsZero_HoldsOnlyForZero()
    {
        Assert.True(Z64.IsZero(Z64.Zero));
        GenZ64.Sample(a =>
        {
            var expected = a.Value == 0;
            return Z64.IsZero(a) == expected;
        });
    }

    [Fact]
    public void IsPositive_AgreesWithSign()
    {
        GenZ64.Sample(a => Z64.IsPositive(a) == (a.Value > 0));
    }

    [Fact]
    public void Subtract_IsAddNegate()
    {
        Gen.Select(GenZ64, GenZ64).Sample((a, b) =>
            Z64.Subtract(a, b) == Z64.Add(a, Z64.Negate(b)));
    }

    [Fact]
    public void Add_OverflowThrows()
    {
        Assert.Throws<OverflowException>(() => Z64.Add(new Z64(long.MaxValue), Z64.One));
    }
}
