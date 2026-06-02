// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;

namespace DbspNet.Tests.Operators;

public class LinearOperatorTests
{
    private static readonly Gen<ZSet<string, Z64>> GenZSet =
        Gen.Select(Gen.OneOfConst("a", "b", "c"), Gen.Long[-3, 3].Select(v => new Z64(v)))
           .Array[0, 5]
           .Select(xs => ZSet.FromEntries(xs));

    [Fact]
    public void MapRows_PointwiseTransform()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            var doubled = b.MapRows(s, x => x * 2);
            oh = b.Output(doubled);
        });

        ih!.Push(ZSet.FromEntries(new[] { (1, new Z64(1)), (2, new Z64(3)) }));
        c.Step();
        Assert.Equal(new Z64(1), oh!.Current.WeightOf(2));
        Assert.Equal(new Z64(3), oh!.Current.WeightOf(4));
    }

    [Fact]
    public void Filter_KeepsOnlyMatchingRows()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            var filtered = b.Filter(s, x => x > 0);
            oh = b.Output(filtered);
        });

        ih!.Push(ZSet.FromEntries(new[] { (-1, new Z64(1)), (0, new Z64(2)), (3, new Z64(4)) }));
        c.Step();
        Assert.Equal(1, oh!.Current.Count);
        Assert.Equal(new Z64(4), oh!.Current.WeightOf(3));
    }

    [Fact]
    public void Union_AddsWeights()
    {
        InputHandle<ZSet<int, Z64>>? ia = null;
        InputHandle<ZSet<int, Z64>>? ib = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h1, s1) = b.ZSetInput<int, Z64>();
            var (h2, s2) = b.ZSetInput<int, Z64>();
            ia = h1;
            ib = h2;
            oh = b.Output(b.Union(s1, s2));
        });

        ia!.Push(ZSet.FromEntries(new[] { (1, new Z64(2)) }));
        ib!.Push(ZSet.FromEntries(new[] { (1, new Z64(3)), (2, new Z64(1)) }));
        c.Step();

        Assert.Equal(new Z64(5), oh!.Current.WeightOf(1));
        Assert.Equal(new Z64(1), oh!.Current.WeightOf(2));
    }

    [Fact]
    public void Difference_Subtracts()
    {
        InputHandle<ZSet<int, Z64>>? ia = null;
        InputHandle<ZSet<int, Z64>>? ib = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h1, s1) = b.ZSetInput<int, Z64>();
            var (h2, s2) = b.ZSetInput<int, Z64>();
            ia = h1;
            ib = h2;
            oh = b.Output(b.Difference(s1, s2));
        });

        ia!.Push(ZSet.Singleton(1, new Z64(5)));
        ib!.Push(ZSet.Singleton(1, new Z64(2)));
        c.Step();
        Assert.Equal(new Z64(3), oh!.Current.WeightOf(1));
    }

    [Fact]
    public void Negate_NegatesWeights()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            oh = b.Output(b.Negate(s));
        });

        ih!.Push(ZSet.FromEntries(new[] { (1, new Z64(2)), (2, new Z64(-3)) }));
        c.Step();
        Assert.Equal(new Z64(-2), oh!.Current.WeightOf(1));
        Assert.Equal(new Z64(3), oh!.Current.WeightOf(2));
    }

    [Fact]
    public void FlatMapRows_ExpandsEachRow()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<int, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;
            var expanded = b.FlatMapRows(s, x => new[] { x, x * 10 });
            oh = b.Output(expanded);
        });

        ih!.Push(ZSet.FromEntries(new[] { (1, new Z64(1)), (2, new Z64(3)) }));
        c.Step();

        Assert.Equal(new Z64(1), oh!.Current.WeightOf(1));
        Assert.Equal(new Z64(1), oh!.Current.WeightOf(10));
        Assert.Equal(new Z64(3), oh!.Current.WeightOf(2));
        Assert.Equal(new Z64(3), oh!.Current.WeightOf(20));
    }

    [Fact]
    public void MapFilterRows_MapsAndDropsInOnePass()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;

            // Keep positives, double them; Keep == false drops the row.
            var fused = b.MapFilterRows<int, string, Z64>(
                s, x => x > 0
                    ? (true, (x * 2).ToString(System.Globalization.CultureInfo.InvariantCulture))
                    : (false, string.Empty));
            oh = b.Output(fused);
        });

        ih!.Push(ZSet.FromEntries(new[] { (-1, new Z64(1)), (0, new Z64(2)), (3, new Z64(4)) }));
        c.Step();

        Assert.Equal(1, oh!.Current.Count);
        Assert.Equal(new Z64(4), oh!.Current.WeightOf("6"));
    }

    [Fact]
    public void MapFilterRows_AccumulatesCollidingOutputs()
    {
        InputHandle<ZSet<int, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            ih = h;

            // Distinct inputs collapse to the same output key — weights must add.
            var fused = b.MapFilterRows<int, string, Z64>(
                s, x => (true, (x % 2).ToString(System.Globalization.CultureInfo.InvariantCulture)));
            oh = b.Output(fused);
        });

        ih!.Push(ZSet.FromEntries(new[] { (1, new Z64(2)), (3, new Z64(5)), (2, new Z64(4)) }));
        c.Step();

        Assert.Equal(new Z64(7), oh!.Current.WeightOf("1")); // 1 and 3
        Assert.Equal(new Z64(4), oh!.Current.WeightOf("0")); // 2
    }

    [Fact]
    public void MapFilterRows_EquivalentToStagedFilterThenMap()
    {
        // A fused map+filter must equal the staged Filter→MapRows chain on any
        // input — the soundness guarantee the SQL compiler relies on.
        static (bool, string) Step(string k) =>
            k == "a" ? (false, string.Empty) : (true, k.ToUpperInvariant());

        GenZSet.Sample(z =>
        {
            var fused = RunThroughBuilder(z, (b, s, emit) =>
                emit(b.MapFilterRows<string, string, Z64>(s, Step)));
            var staged = RunThroughBuilder(z, (b, s, emit) =>
                emit(b.MapRows(b.Filter(s, k => k != "a"), k => k.ToUpperInvariant())));
            return fused.Equals(staged);
        });
    }

    // --- Linearity laws: Q(a + b) = Q(a) + Q(b) and Q(n·a) = n·Q(a) ---

    private static ZSet<string, Z64> RunThroughBuilder(
        ZSet<string, Z64> input,
        Action<CircuitBuilder, Stream<ZSet<string, Z64>>, Action<Stream<ZSet<string, Z64>>>> plumb)
    {
        InputHandle<ZSet<string, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<string, Z64>();
            ih = h;
            plumb(b, s, stream => oh = b.Output(stream));
        });

        ih!.Push(input);
        c.Step();
        return oh!.Current;
    }

    private static ZSet<string, Z64> ApplyMap(ZSet<string, Z64> z) =>
        RunThroughBuilder(z, (b, s, emit) => emit(b.MapRows(s, k => k.ToUpperInvariant())));

    private static ZSet<string, Z64> ApplyFilter(ZSet<string, Z64> z) =>
        RunThroughBuilder(z, (b, s, emit) => emit(b.Filter(s, k => k != "a")));

    [Fact]
    public void MapRows_IsLinear()
    {
        Gen.Select(GenZSet, GenZSet).Sample((a, b) =>
            ApplyMap(a + b).Equals(ApplyMap(a) + ApplyMap(b)));
    }

    [Fact]
    public void Filter_IsLinear()
    {
        Gen.Select(GenZSet, GenZSet).Sample((a, b) =>
            ApplyFilter(a + b).Equals(ApplyFilter(a) + ApplyFilter(b)));
    }

    [Fact]
    public void MapRows_IsHomogeneous()
    {
        var smallScalar = Gen.Long[-3, 3].Select(v => new Z64(v));
        Gen.Select(GenZSet, smallScalar).Sample((z, n) =>
            ApplyMap(z.ScalarMultiply(n)).Equals(ApplyMap(z).ScalarMultiply(n)));
    }

    [Fact]
    public void Filter_IsHomogeneous()
    {
        var smallScalar = Gen.Long[-3, 3].Select(v => new Z64(v));
        Gen.Select(GenZSet, smallScalar).Sample((z, n) =>
            ApplyFilter(z.ScalarMultiply(n)).Equals(ApplyFilter(z).ScalarMultiply(n)));
    }

    [Fact]
    public void Union_IsLinear()
    {
        // Union is LINEAR (not bilinear) when viewed as a function of the
        // pair (a,b) with pairwise addition:
        //     U(a1 + a2, b1 + b2) = U(a1, b1) + U(a2, b2)
        Gen.Select(GenZSet, GenZSet, GenZSet, GenZSet).Sample((a1, a2, b1, b2) =>
        {
            static ZSet<string, Z64> U(ZSet<string, Z64> x, ZSet<string, Z64> y)
            {
                InputHandle<ZSet<string, Z64>>? h1 = null;
                InputHandle<ZSet<string, Z64>>? h2 = null;
                OutputHandle<ZSet<string, Z64>>? oh = null;
                var c = RootCircuit.Build(b =>
                {
                    var (hx, sx) = b.ZSetInput<string, Z64>();
                    var (hy, sy) = b.ZSetInput<string, Z64>();
                    h1 = hx;
                    h2 = hy;
                    oh = b.Output(b.Union(sx, sy));
                });

                h1!.Push(x);
                h2!.Push(y);
                c.Step();
                return oh!.Current;
            }

            return U(a1 + a2, b1 + b2).Equals(U(a1, b1) + U(a2, b2));
        });
    }

    [Fact]
    public void Negate_IsItsOwnInverse()
    {
        GenZSet.Sample(z =>
        {
            InputHandle<ZSet<string, Z64>>? ih = null;
            OutputHandle<ZSet<string, Z64>>? oh = null;
            var c = RootCircuit.Build(b =>
            {
                var (h, s) = b.ZSetInput<string, Z64>();
                ih = h;
                var twice = b.Negate(b.Negate(s));
                oh = b.Output(twice);
            });

            ih!.Push(z);
            c.Step();
            return oh!.Current.Equals(z);
        });
    }
}
