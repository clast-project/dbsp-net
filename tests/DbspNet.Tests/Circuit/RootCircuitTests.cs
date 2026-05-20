// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;

namespace DbspNet.Tests.Circuit;

public class RootCircuitTests
{
    private static (T A, T B) AddPair<T>(T a, T b) => (a, b);

    [Fact]
    public void Identity_PipeOutputsInput()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (handle, stream) = b.Input<int>(0, (x, y) => x + y);
            ih = handle;
            oh = b.Output(stream);
        });

        ih!.Push(5);
        c.Step();
        Assert.Equal(5, oh!.Current);
        Assert.Equal(1, c.TickCount);
    }

    [Fact]
    public void Apply_AppliesTransform()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            var doubled = b.Apply(s, x => x * 2);
            oh = b.Output(doubled);
        });

        ih!.Push(7);
        c.Step();
        Assert.Equal(14, oh!.Current);
    }

    [Fact]
    public void Apply2_CombinesTwoStreams()
    {
        InputHandle<int>? ai = null;
        InputHandle<int>? bi = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (ah, a) = b.Input<int>(0, (x, y) => x + y);
            var (bh, bs) = b.Input<int>(0, (x, y) => x + y);
            ai = ah;
            bi = bh;
            var sum = b.Apply(a, bs, (x, y) => x + y);
            oh = b.Output(sum);
        });

        ai!.Push(3);
        bi!.Push(4);
        c.Step();
        Assert.Equal(7, oh!.Current);
    }

    [Fact]
    public void Chain_AppliesSequentially()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            var x2 = b.Apply(s, v => v * 2);
            var x2p1 = b.Apply(x2, v => v + 1);
            oh = b.Output(x2p1);
        });

        ih!.Push(5);
        c.Step();
        Assert.Equal(11, oh!.Current); // (5 * 2) + 1
    }

    [Fact]
    public void MultiplePushesBeforeStep_AreMergedViaMergeFunction()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y); // merge = add
            ih = h;
            oh = b.Output(s);
        });

        ih!.Push(2);
        ih!.Push(3);
        ih!.Push(4);
        c.Step();
        Assert.Equal(9, oh!.Current);
    }

    [Fact]
    public void NoPushBeforeStep_EmitsZero()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            oh = b.Output(s);
        });

        ih!.Push(42);
        c.Step();
        Assert.Equal(42, oh!.Current);
        // Next tick with no push: zero resets
        c.Step();
        Assert.Equal(0, oh!.Current);
    }

    [Fact]
    public void Delay_EmitsInitialOnTickZero_AndLastTickValueThereafter()
    {
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            var delayed = b.Delay(s, initial: -1);
            oh = b.Output(delayed);
        });

        // Tick 1: input=10, output=initial=-1
        ih!.Push(10);
        c.Step();
        Assert.Equal(-1, oh!.Current);

        // Tick 2: input=20, output=10 (prev tick)
        ih!.Push(20);
        c.Step();
        Assert.Equal(10, oh!.Current);

        // Tick 3: no push (input=0), output=20 (prev tick)
        c.Step();
        Assert.Equal(20, oh!.Current);

        // Tick 4: no push (input=0), output=0 (prev tick's zero)
        c.Step();
        Assert.Equal(0, oh!.Current);
    }

    [Fact]
    public void Differentiation_YieldsInputMinusDelayed()
    {
        // D(x)[t] = x[t] - x[t-1]: a hand-built "differentiate" operator.
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (h, s) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            var prev = b.Delay(s, 0);
            var diff = b.Apply(s, prev, (a, p) => a - p);
            oh = b.Output(diff);
        });

        // Feed 5, 7, 7, 3 — deltas should be 5, 2, 0, -4
        ih!.Push(5); c.Step(); Assert.Equal(5, oh!.Current);
        ih!.Push(7); c.Step(); Assert.Equal(2, oh!.Current);
        ih!.Push(7); c.Step(); Assert.Equal(0, oh!.Current);
        ih!.Push(3); c.Step(); Assert.Equal(-4, oh!.Current);
    }

    [Fact]
    public void StatefulClosure_YieldsRunningSum()
    {
        // I(x)[t] = x[0] + x[1] + ... + x[t]. Nested/feedback circuits are not supported
        // in v1, so we express the running sum via a stateful closure inside Apply —
        // equivalent in effect and all we need to exercise the circuit step model.
        InputHandle<int>? ih = null;
        OutputHandle<int>? oh = null;
        var runningSum = 0;
        var c = RootCircuit.Build(b =>
        {
            var (h, input) = b.Input<int>(0, (x, y) => x + y);
            ih = h;
            var running = b.Apply(input, v =>
            {
                runningSum += v;
                return runningSum;
            });
            oh = b.Output(running);
        });

        ih!.Push(5);
        c.Step();
        Assert.Equal(5, oh!.Current);

        ih!.Push(3);
        c.Step();
        Assert.Equal(8, oh!.Current);

        ih!.Push(2);
        c.Step();
        Assert.Equal(10, oh!.Current);
    }

    [Fact]
    public void TickCount_AdvancesByOnePerStep()
    {
        var c = RootCircuit.Build(_ => { });
        Assert.Equal(0, c.TickCount);
        c.Step();
        Assert.Equal(1, c.TickCount);
        c.Step();
        c.Step();
        Assert.Equal(3, c.TickCount);
    }
}
