// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Tests.Operators.Stateful;

public class IncrementalJoinTests
{
    private sealed record Person(int Id, string Name);
    private sealed record Role(int PersonId, string RoleName);

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<Person, Z64>> Left,
        InputHandle<ZSet<Role, Z64>> Right,
        OutputHandle<ZSet<(int Id, string Name, string RoleName), Z64>> Output)
        BuildJoinCircuit()
    {
        InputHandle<ZSet<Person, Z64>>? li = null;
        InputHandle<ZSet<Role, Z64>>? ri = null;
        OutputHandle<ZSet<(int Id, string Name, string RoleName), Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<Person, Z64>();
            var (rh, rs) = b.ZSetInput<Role, Z64>();
            li = lh;
            ri = rh;
            var lIdx = b.IndexBy(ls, p => p.Id);
            var rIdx = b.IndexBy(rs, r => r.PersonId);
            var joined = b.IncrementalInnerJoin(lIdx, rIdx, (_, p, r) => (p.Id, p.Name, r.RoleName));
            oh = b.Output(joined);
        });
        return (c, li!, ri!, oh!);
    }

    [Fact]
    public void EmptyInputs_EmitEmptyOutput()
    {
        var (c, _, _, oh) = BuildJoinCircuit();
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void BothSidesInSameTick_EmitsCrossJoinOfDeltas()
    {
        var (c, left, right, oh) = BuildJoinCircuit();
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();

        Assert.Equal(1, oh.Current.Count);
        Assert.Equal(Z64.One, oh.Current.WeightOf((1, "alice", "admin")));
    }

    [Fact]
    public void LateArrivalOnRightSide_EmitsHistoricalMatches()
    {
        var (c, left, right, oh) = BuildJoinCircuit();

        // Tick 1: add person, no roles yet.
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        c.Step();
        Assert.True(oh.Current.IsEmpty);

        // Tick 2: add role — should join against historical person.
        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf((1, "alice", "admin")));
    }

    [Fact]
    public void RetractionOfPerson_EmitsMatchingRowRetractions()
    {
        var (c, left, right, oh) = BuildJoinCircuit();

        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.FromEntries(new[]
        {
            (new Role(1, "admin"), Z64.One),
            (new Role(1, "reader"), Z64.One),
        }));
        c.Step();
        Assert.Equal(2, oh.Current.Count);

        // Tick 2: retract the person.
        left.Push(ZSet.Singleton(new Person(1, "alice"), new Z64(-1)));
        c.Step();

        Assert.Equal(new Z64(-1), oh.Current.WeightOf((1, "alice", "admin")));
        Assert.Equal(new Z64(-1), oh.Current.WeightOf((1, "alice", "reader")));
    }

    [Fact]
    public void AccumulatedIncrementalOutput_EqualsBatchJoinOverCumulativeInput()
    {
        var (c, left, right, oh) = BuildJoinCircuit();

        var lDeltas = new[]
        {
            ZSet.FromEntries(new[] { (new Person(1, "alice"), Z64.One), (new Person(2, "bob"), Z64.One) }),
            ZSet.FromEntries(new[] { (new Person(1, "alice"), new Z64(-1)) }),  // retract alice
            ZSet.FromEntries(new[] { (new Person(3, "carol"), Z64.One) }),
        };
        var rDeltas = new[]
        {
            ZSet.FromEntries(new[] { (new Role(1, "admin"), Z64.One) }),
            ZSet.FromEntries(new[] { (new Role(2, "reader"), Z64.One) }),
            ZSet.FromEntries(new[] { (new Role(3, "author"), Z64.One) }),
        };

        var lAcc = ZSet<Person, Z64>.Empty;
        var rAcc = ZSet<Role, Z64>.Empty;
        var outAcc = ZSet<(int Id, string Name, string RoleName), Z64>.Empty;

        for (int i = 0; i < lDeltas.Length; i++)
        {
            left.Push(lDeltas[i]);
            right.Push(rDeltas[i]);
            c.Step();
            outAcc = outAcc.Plus(oh.Current);
            lAcc = lAcc + lDeltas[i];
            rAcc = rAcc + rDeltas[i];
        }

        // Batch oracle: nested-loop join over the accumulated state.
        var expected = new ZSetBuilder<(int Id, string Name, string RoleName), Z64>();
        foreach (var (p, pw) in lAcc)
        {
            foreach (var (r, rw) in rAcc)
            {
                if (p.Id == r.PersonId)
                {
                    expected.Add((p.Id, p.Name, r.RoleName), Z64.Multiply(pw, rw));
                }
            }
        }

        Assert.Equal(expected.Build(), outAcc);
    }
}
