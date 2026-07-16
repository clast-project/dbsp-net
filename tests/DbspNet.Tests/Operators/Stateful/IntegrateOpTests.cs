// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;

namespace DbspNet.Tests.Operators.Stateful;

/// <summary>
/// The DBSP integration operator <c>I</c> at the output boundary: <c>View</c> is
/// the running sum of all deltas (the full materialized view) while the delta
/// still flows through unchanged. Multi-weight and drive-to-zero cases are covered
/// here (the SQL surface only pushes ±1); see StoredOutputTests for the
/// end-to-end differential check.
/// </summary>
public class IntegrateOpTests
{
    private static (RootCircuit Circuit, InputHandle<ZSet<string, Z64>> Input,
        OutputHandle<ZSet<string, Z64>> Delta, IntegratedViewHandle<string> View)
        BuildIntegrateCircuit()
    {
        InputHandle<ZSet<string, Z64>>? ih = null;
        OutputHandle<ZSet<string, Z64>>? delta = null;
        IntegratedViewHandle<string>? view = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.ZSetInput<string, Z64>();
            ih = h;
            var integrated = b.Integrate(s);
            view = integrated.View;
            delta = b.Output(integrated.Output);
        });
        return (c, ih!, delta!, view!);
    }

    [Fact]
    public void ViewAccumulatesDeltas_WhileDeltaFlowsThrough()
    {
        var (c, ih, delta, view) = BuildIntegrateCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(1)));
        c.Step();
        // Delta is this tick's change; View is the running total (same here).
        Assert.Equal(Z64.One, delta.Current.WeightOf("a"));
        Assert.Equal(Z64.One, view.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("b", new Z64(1)));
        c.Step();
        // Delta carries only b; View retains a and gains b.
        Assert.Equal(Z64.One, delta.Current.WeightOf("b"));
        Assert.Equal(Z64.Zero, delta.Current.WeightOf("a"));
        Assert.Equal(Z64.One, view.Current.WeightOf("a"));
        Assert.Equal(Z64.One, view.Current.WeightOf("b"));
        Assert.Equal(2, view.Current.Count);
    }

    [Fact]
    public void ViewKeepsMultiplicity()
    {
        var (c, ih, _, view) = BuildIntegrateCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(3)));
        c.Step();
        ih.Push(ZSet.Singleton("a", new Z64(2)));
        c.Step();

        Assert.Equal(new Z64(5), view.Current.WeightOf("a")); // bag semantics retained
    }

    [Fact]
    public void RowDrivenToZeroLeavesTheView()
    {
        var (c, ih, _, view) = BuildIntegrateCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(2)));
        c.Step();
        Assert.Equal(new Z64(2), view.Current.WeightOf("a"));

        ih.Push(ZSet.Singleton("a", new Z64(-2))); // retract both copies
        c.Step();
        Assert.Equal(Z64.Zero, view.Current.WeightOf("a"));
        Assert.True(view.Current.IsEmpty); // zero-is-absent invariant
    }

    [Fact]
    public void EmptyTickLeavesViewUnchanged()
    {
        var (c, ih, delta, view) = BuildIntegrateCircuit();

        ih.Push(ZSet.Singleton("a", new Z64(1)));
        c.Step();

        c.Step(); // no push
        Assert.True(delta.Current.IsEmpty);
        Assert.Equal(Z64.One, view.Current.WeightOf("a"));
        Assert.Equal(1, view.Current.Count);
    }
}
