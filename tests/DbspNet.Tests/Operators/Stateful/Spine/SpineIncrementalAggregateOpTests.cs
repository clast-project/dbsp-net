using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

/// <summary>
/// Spot-checks and flat-vs-spine parity for
/// <see cref="SpineIncrementalAggregateOp{TKey,TValue,TOut}"/>.
/// Observable behaviour must be indistinguishable from the flat op.
/// </summary>
public class SpineIncrementalAggregateOpTests
{
    private sealed record Row(string Dept, long Salary);

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<Row, Z64>> Input,
        OutputHandle<ZSet<(string Key, TOut Value), Z64>> Output)
        BuildSpineAggregateCircuit<TOut>(IAggregator<long, TOut> aggregator)
        where TOut : notnull
    {
        InputHandle<ZSet<Row, Z64>>? ih = null;
        OutputHandle<ZSet<(string Key, TOut Value), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Row, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, r => r.Dept, r => r.Salary);
            var aggregated = b.SpineIncrementalAggregate(grouped, aggregator);
            oh = b.Output(aggregated);
        });
        return (c, ih!, oh!);
    }

    [Fact]
    public void Sum_LaterInsertion_EmitsRetractOldPlusInsertNew()
    {
        var (c, ih, oh) = BuildSpineAggregateCircuit(new SumAggregator<long>());
        ih.Push(ZSet.Singleton(new Row("eng", 100L), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 100L)));

        ih.Push(ZSet.Singleton(new Row("eng", 50L), Z64.One));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 100L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 150L)));
    }

    [Fact]
    public void Count_GroupCollapsedToEmpty_EmitsOnlyRetraction()
    {
        var (c, ih, oh) = BuildSpineAggregateCircuit(new CountStarAggregator<long>());

        ih.Push(ZSet.Singleton(new Row("eng", 100L), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 1L)));

        ih.Push(ZSet.Singleton(new Row("eng", 100L), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 1L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 0L)));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void MatchesFlatAggregateAcrossCompactionThresholds(int batchesPerLevel)
    {
        // Property: the spine and flat aggregate must emit identical
        // per-tick deltas on the same input sequence, regardless of the
        // spine's compaction policy.
        var rng = new Random(Seed: 37 + batchesPerLevel);

        InputHandle<ZSet<Row, Z64>>? flatIn = null;
        OutputHandle<ZSet<(string Key, long Value), Z64>>? flatOut = null;
        var flatCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Row, Z64>();
            flatIn = h;
            var grouped = b.GroupProject(s, r => r.Dept, r => r.Salary);
            flatOut = b.Output(b.IncrementalAggregate(grouped, new SumAggregator<long>()));
        });

        InputHandle<ZSet<Row, Z64>>? spineIn = null;
        OutputHandle<ZSet<(string Key, long Value), Z64>>? spineOut = null;
        var spineCircuit = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Row, Z64>();
            spineIn = h;
            var grouped = b.GroupProject(s, r => r.Dept, r => r.Salary);
            spineOut = b.Output(b.SpineIncrementalAggregate(
                grouped, new SumAggregator<long>(),
                compactionStrategy: new TieredCompactionStrategy(batchesPerLevel)));
        });

        for (var step = 0; step < 150; step++)
        {
            var delta = RandomRowDelta(rng, deptCount: 5, salaryRange: 1000, maxEntries: 4);
            flatIn!.Push(delta);
            spineIn!.Push(delta);
            flatCircuit.Step();
            spineCircuit.Step();

            Assert.Equal(flatOut!.Current, spineOut!.Current);
        }
    }

    private static ZSet<Row, Z64> RandomRowDelta(Random rng, int deptCount, int salaryRange, int maxEntries)
    {
        var entries = rng.Next(maxEntries + 1);
        if (entries == 0)
        {
            return ZSet<Row, Z64>.Empty;
        }

        var b = new ZSetBuilder<Row, Z64>();
        for (var i = 0; i < entries; i++)
        {
            var dept = $"d{rng.Next(deptCount)}";
            var salary = (long)rng.Next(salaryRange);
            var w = rng.Next(-2, 4);
            if (w == 0)
            {
                continue;
            }

            b.Add(new Row(dept, salary), new Z64(w));
        }

        return b.Build();
    }
}
