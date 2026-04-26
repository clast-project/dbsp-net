using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Tests.Operators.Stateful;

public class DistinctTests
{
    private static (RootCircuit Circuit, InputHandle<ZSet<string, Z64>> Input, OutputHandle<ZSet<string, Z64>> Output)
        BuildDistinctCircuit()
    {
        InputHandle<ZSet<string, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<string, Z64>();
            ih = h;
            oh = b.Output(b.Distinct(s));
        });
        return (c, ih!, oh!);
    }

    [Fact]
    public void EmitsPlusOneOnFirstInsertion()
    {
        var (c, ih, oh) = BuildDistinctCircuit();
        ih.Push(ZSet.Singleton("a", new Z64(3)));
        c.Step();

        Assert.Equal(1, oh.Current.Count);
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));
    }

    [Fact]
    public void DoesNothingWhileKeyRemainsPositive()
    {
        var (c, ih, oh) = BuildDistinctCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(2)));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(5))); // still positive, more copies
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void EmitsMinusOneWhenKeyRetractedToZero()
    {
        var (c, ih, oh) = BuildDistinctCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(1)));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(-1)));
        c.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf("a"));
    }

    [Fact]
    public void DoesNothingIfKeyNeverCrossesZero()
    {
        // Sequence of pushes that leaves "a" at weight 3 the whole time (no
        // retractions into nothing).
        var (c, ih, oh) = BuildDistinctCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(3)));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(-2))); // weight becomes 1 — still positive
        c.Step();
        Assert.True(oh.Current.IsEmpty);

        ih.Push(ZSet.Singleton("a", new Z64(4))); // weight becomes 5 — still positive
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void CumulativeOutputTracksSetIndicator()
    {
        // Law: the running sum of outputs equals the indicator multiset
        // (weight 1 for present keys, 0 for absent), where "present" means
        // cumulative input weight > 0.
        var (c, ih, oh) = BuildDistinctCircuit();

        var deltas = new[]
        {
            ZSet.FromEntries(new[] { ("a", new Z64(2)), ("b", new Z64(1)) }),
            ZSet.FromEntries(new[] { ("a", new Z64(-1)) }),
            ZSet.FromEntries(new[] { ("c", new Z64(1)), ("b", new Z64(-1)) }),
        };

        var accumulatedInput = ZSet<string, Z64>.Empty;
        var accumulatedOutput = ZSet<string, Z64>.Empty;
        foreach (var d in deltas)
        {
            ih.Push(d);
            c.Step();
            accumulatedInput = accumulatedInput + d;
            accumulatedOutput = accumulatedOutput + oh.Current;
        }

        // Expected output = indicator set: keys with positive cumulative weight have weight 1.
        var expectedBuilder = new ZSetBuilder<string, Z64>();
        foreach (var (k, w) in accumulatedInput)
        {
            if (Z64.IsPositive(w))
            {
                expectedBuilder.Add(k, Z64.One);
            }
        }

        Assert.Equal(expectedBuilder.Build(), accumulatedOutput);
    }
}
