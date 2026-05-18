using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Mirrors <c>DistinctTests</c> against
/// <see cref="SpineDistinctOp{TKey,TWeight}"/>: same shaped inputs,
/// same expected outputs. The spine variant should be observationally
/// indistinguishable from the flat distinct.
/// </summary>
public class SpineDistinctOpTests
{
    private static (RootCircuit Circuit, InputHandle<ZSet<string, Z64>> Input, OutputHandle<ZSet<string, Z64>> Output)
        BuildSpineDistinctCircuit(ICompactionStrategy? strategy = null)
    {
        InputHandle<ZSet<string, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<string, Z64>();
            ih = h;
            oh = b.Output(b.SpineDistinct(s, strategy));
        });
        return (c, ih!, oh!);
    }

    [Fact]
    public void EmitsPlusOneOnFirstInsertion()
    {
        var (c, ih, oh) = BuildSpineDistinctCircuit();
        ih.Push(ZSet.Singleton("a", new Z64(3)));
        c.Step();

        Assert.Equal(1, oh.Current.Count);
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));
    }

    [Fact]
    public void DoesNothingWhileKeyRemainsPositive()
    {
        var (c, ih, oh) = BuildSpineDistinctCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(2)));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(5)));
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void EmitsMinusOneWhenKeyRetractedToZero()
    {
        var (c, ih, oh) = BuildSpineDistinctCircuit();

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
        var (c, ih, oh) = BuildSpineDistinctCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(3)));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(-2)));
        c.Step();
        Assert.True(oh.Current.IsEmpty);

        ih.Push(ZSet.Singleton("a", new Z64(4)));
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void CumulativeOutputTracksSetIndicator()
    {
        var (c, ih, oh) = BuildSpineDistinctCircuit();

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

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void MatchesFlatDistinctAcrossCompactionThresholds(int batchesPerLevel)
    {
        // Property: regardless of compaction policy, SpineDistinct must
        // produce identical outputs to the flat Distinct on the same
        // input sequence.
        var rng = new Random(Seed: 19 + batchesPerLevel);

        // Build two parallel circuits — flat and spine — and tick them
        // through the same delta sequence.
        InputHandle<ZSet<int, Z64>>? flatIn = null;
        OutputHandle<ZSet<int, Z64>>? flatOut = null;
        var flatCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            flatIn = h;
            flatOut = b.Output(b.Distinct(s));
        });

        InputHandle<ZSet<int, Z64>>? spineIn = null;
        OutputHandle<ZSet<int, Z64>>? spineOut = null;
        var spineCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<int, Z64>();
            spineIn = h;
            spineOut = b.Output(b.SpineDistinct(s, new TieredCompactionStrategy(batchesPerLevel)));
        });

        for (var step = 0; step < 200; step++)
        {
            var delta = RandomDelta(rng, keySpace: 20, maxEntries: 5);
            flatIn!.Push(delta);
            spineIn!.Push(delta);
            flatCircuit.Step();
            spineCircuit.Step();

            Assert.Equal(flatOut!.Current, spineOut!.Current);
        }
    }

    private static ZSet<int, Z64> RandomDelta(Random rng, int keySpace, int maxEntries)
    {
        var entries = rng.Next(maxEntries + 1);
        if (entries == 0)
        {
            return ZSet<int, Z64>.Empty;
        }

        var b = new ZSetBuilder<int, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var k = rng.Next(keySpace);
            var w = rng.Next(-3, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(k, new Z64(w));
        }

        return b.Build();
    }
}
