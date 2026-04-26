using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Aggregators;

namespace DbspNet.Tests.Operators.Stateful;

public class IncrementalAggregateTests
{
    private sealed record Row(string Dept, long Salary);

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<Row, Z64>> Input,
        OutputHandle<ZSet<(string Key, TOut Value), Z64>> Output)
        BuildAggregateCircuit<TOut>(IAggregator<long, TOut> aggregator)
        where TOut : notnull
    {
        InputHandle<ZSet<Row, Z64>>? ih = null;
        OutputHandle<ZSet<(string Key, TOut Value), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<Row, Z64>();
            ih = h;
            var grouped = b.GroupProject(s, r => r.Dept, r => r.Salary);
            var aggregated = b.IncrementalAggregate(grouped, aggregator);
            oh = b.Output(aggregated);
        });
        return (c, ih!, oh!);
    }

    [Fact]
    public void Sum_SingleInsertion_EmitsGroupAggregate()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new SumAggregator<long>());
        ih.Push(ZSet.FromEntries(new[]
        {
            (new Row("eng", 100L), Z64.One),
            (new Row("eng", 200L), Z64.One),
        }));
        c.Step();

        Assert.Equal(1, oh.Current.Count);
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 300L)));
    }

    [Fact]
    public void Sum_LaterInsertion_EmitsRetractOldPlusInsertNew()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new SumAggregator<long>());
        ih.Push(ZSet.Singleton(new Row("eng", 100L), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 100L)));

        ih.Push(ZSet.Singleton(new Row("eng", 50L), Z64.One));
        c.Step();

        // Retraction of old total 100 and insertion of new total 150.
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 100L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 150L)));
    }

    [Fact]
    public void Count_PreservesGroupAfterRetraction()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new CountStarAggregator<long>());

        ih.Push(ZSet.FromEntries(new[]
        {
            (new Row("eng", 100L), Z64.One),
            (new Row("eng", 200L), Z64.One),
        }));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 2L)));

        ih.Push(ZSet.Singleton(new Row("eng", 100L), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 2L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 1L)));
    }

    [Fact]
    public void Count_GroupCollapsedToEmpty_EmitsOnlyRetraction()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new CountStarAggregator<long>());

        ih.Push(ZSet.Singleton(new Row("eng", 100L), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 1L)));

        ih.Push(ZSet.Singleton(new Row("eng", 100L), new Z64(-1)));
        c.Step();

        // Group goes to empty. For COUNT(*) the "empty" aggregate is 0, not None,
        // so we get retract(1) + insert(0).
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 1L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 0L)));
    }

    [Fact]
    public void Min_HandlesRetractionOfCurrentMinimum()
    {
        var (c, ih, oh) = BuildAggregateCircuit(MinMaxAggregator<long>.Min());

        ih.Push(ZSet.FromEntries(new[]
        {
            (new Row("eng", 100L), Z64.One),
            (new Row("eng", 200L), Z64.One),
            (new Row("eng", 300L), Z64.One),
        }));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 100L)));

        // Retract the minimum 100 — the new min is 200.
        ih.Push(ZSet.Singleton(new Row("eng", 100L), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 100L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 200L)));
    }

    [Fact]
    public void Max_HandlesRetractionOfCurrentMaximum()
    {
        var (c, ih, oh) = BuildAggregateCircuit(MinMaxAggregator<long>.Max());

        ih.Push(ZSet.FromEntries(new[]
        {
            (new Row("eng", 10L), Z64.One),
            (new Row("eng", 20L), Z64.One),
            (new Row("eng", 30L), Z64.One),
        }));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 30L)));

        ih.Push(ZSet.Singleton(new Row("eng", 30L), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 30L)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 20L)));
    }

    [Fact]
    public void Avg_ComputesMeanAndRevisesOnRetraction()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new AvgAggregator<long>());

        ih.Push(ZSet.FromEntries(new[]
        {
            (new Row("eng", 100L), Z64.One),
            (new Row("eng", 200L), Z64.One),
        }));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 150.0)));

        ih.Push(ZSet.Singleton(new Row("eng", 200L), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf(("eng", 150.0)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(("eng", 100.0)));
    }

    [Fact]
    public void Sum_AccumulatedOutputMatchesBatchRecomputation()
    {
        var (c, ih, oh) = BuildAggregateCircuit(new SumAggregator<long>());

        var deltas = new[]
        {
            ZSet.FromEntries(new[] { (new Row("eng", 100L), Z64.One), (new Row("sales", 50L), Z64.One) }),
            ZSet.FromEntries(new[] { (new Row("eng", 200L), Z64.One) }),
            ZSet.FromEntries(new[] { (new Row("eng", 100L), new Z64(-1)) }),
            ZSet.FromEntries(new[] { (new Row("sales", 75L), Z64.One) }),
        };

        var inputAcc = ZSet<Row, Z64>.Empty;
        var outputAcc = ZSet<(string Key, long Value), Z64>.Empty;
        foreach (var d in deltas)
        {
            ih.Push(d);
            c.Step();
            outputAcc = outputAcc.Plus(oh.Current);
            inputAcc = inputAcc + d;
        }

        // Batch oracle: group by Dept, sum Salary * weight per group.
        var expected = new ZSetBuilder<(string Key, long Value), Z64>();
        var groupSums = new Dictionary<string, long>();
        foreach (var (row, w) in inputAcc)
        {
            if (!groupSums.TryGetValue(row.Dept, out var s))
            {
                s = 0;
            }

            groupSums[row.Dept] = s + row.Salary * w.Value;
        }

        foreach (var (dept, s) in groupSums)
        {
            expected.Add((dept, s), Z64.One);
        }

        Assert.Equal(expected.Build(), outputAcc);
    }
}
