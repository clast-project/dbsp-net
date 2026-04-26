using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;

namespace DbspNet.Tests.Operators.Stateful;

public class IncrementalLeftJoinTests
{
    private sealed record Person(int Id, string Name);
    private sealed record Role(int PersonId, string RoleName);

    // Out = (left.Id, left.Name, right.RoleName-or-NULL).
    // Using `string?` for right.RoleName lets us encode the NULL-padded case directly.
    private sealed record OutRow(int Id, string Name, string? RoleName);

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<Person, Z64>> Left,
        InputHandle<ZSet<Role, Z64>> Right,
        OutputHandle<ZSet<OutRow, Z64>> Output)
        BuildLeftJoinCircuit()
    {
        InputHandle<ZSet<Person, Z64>>? li = null;
        InputHandle<ZSet<Role, Z64>>? ri = null;
        OutputHandle<ZSet<OutRow, Z64>>? oh = null;
        var c = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<Person, Z64>();
            var (rh, rs) = b.ZSetInput<Role, Z64>();
            li = lh;
            ri = rh;
            var lIdx = b.IndexBy(ls, p => p.Id);
            var rIdx = b.IndexBy(rs, r => r.PersonId);
            var joined = b.IncrementalLeftJoin(
                lIdx,
                rIdx,
                joinCombine: (_, p, r) => new OutRow(p.Id, p.Name, r.RoleName),
                nullPadCombine: (_, p) => new OutRow(p.Id, p.Name, RoleName: null));
            oh = b.Output(joined);
        });
        return (c, li!, ri!, oh!);
    }

    [Fact]
    public void EmptyInputs_EmitEmptyOutput()
    {
        var (c, _, _, oh) = BuildLeftJoinCircuit();
        c.Step();
        Assert.True(oh.Current.IsEmpty);
    }

    [Fact]
    public void LeftOnly_EmitsNullPaddedRow()
    {
        var (c, left, _, oh) = BuildLeftJoinCircuit();
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, "alice", null)));
        Assert.Equal(1, oh.Current.Count);
    }

    [Fact]
    public void GainedMatch_RetractsNullPadded_EmitsJoined()
    {
        var (c, left, right, oh) = BuildLeftJoinCircuit();

        // Tick 1: left-only → NULL-padded.
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, "alice", null)));

        // Tick 2: right arrives — retract NULL-padded, emit joined.
        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, "alice", null)));
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, "alice", "admin")));
    }

    [Fact]
    public void LostMatch_RetractsJoined_EmitsNullPadded()
    {
        var (c, left, right, oh) = BuildLeftJoinCircuit();

        // Tick 1: establish matched state.
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, "alice", "admin")));

        // Tick 2: retract the only right-side row → key becomes unmatched.
        right.Push(ZSet.Singleton(new Role(1, "admin"), new Z64(-1)));
        c.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, "alice", "admin")));
        Assert.Equal(Z64.One, oh.Current.WeightOf(new OutRow(1, "alice", null)));
    }

    [Fact]
    public void RetractionOfLeftRow_WhileMatched_RetractsAllJoinedRows()
    {
        var (c, left, right, oh) = BuildLeftJoinCircuit();

        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.FromEntries(new[]
        {
            (new Role(1, "admin"), Z64.One),
            (new Role(1, "reader"), Z64.One),
        }));
        c.Step();
        Assert.Equal(2, oh.Current.Count);

        left.Push(ZSet.Singleton(new Person(1, "alice"), new Z64(-1)));
        c.Step();
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, "alice", "admin")));
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, "alice", "reader")));
    }

    [Fact]
    public void KeyWithMultipleMatchesLosesOne_StillMatched_EmitsInnerDelta()
    {
        var (c, left, right, oh) = BuildLeftJoinCircuit();

        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.FromEntries(new[]
        {
            (new Role(1, "admin"), Z64.One),
            (new Role(1, "reader"), Z64.One),
        }));
        c.Step();

        // Retract one right-side role; key still matched (reader remains).
        right.Push(ZSet.Singleton(new Role(1, "admin"), new Z64(-1)));
        c.Step();

        // Should NOT emit a NULL-padded row for alice (she's still matched).
        Assert.Equal(new Z64(-1), oh.Current.WeightOf(new OutRow(1, "alice", "admin")));
        Assert.Equal(Z64.Zero, oh.Current.WeightOf(new OutRow(1, "alice", null)));
    }

    [Fact]
    public void AccumulatedIncrementalOutput_EqualsBatchLeftJoin()
    {
        var (c, left, right, oh) = BuildLeftJoinCircuit();

        var lDeltas = new[]
        {
            ZSet.FromEntries(new[] { (new Person(1, "alice"), Z64.One), (new Person(2, "bob"), Z64.One) }),
            ZSet.FromEntries(new[] { (new Person(3, "carol"), Z64.One) }),
            ZSet.FromEntries(new[] { (new Person(2, "bob"), new Z64(-1)) }),     // retract bob
        };
        var rDeltas = new[]
        {
            ZSet.FromEntries(new[] { (new Role(1, "admin"), Z64.One) }),          // alice matches
            ZSet.FromEntries(new[] { (new Role(3, "author"), Z64.One) }),         // carol matches
            ZSet.FromEntries(new[] { (new Role(3, "author"), new Z64(-1)) }),     // carol now unmatched
        };

        var lAcc = ZSet<Person, Z64>.Empty;
        var rAcc = ZSet<Role, Z64>.Empty;
        var outAcc = ZSet<OutRow, Z64>.Empty;

        for (var i = 0; i < lDeltas.Length; i++)
        {
            left.Push(lDeltas[i]);
            right.Push(rDeltas[i]);
            c.Step();
            outAcc = outAcc.Plus(oh.Current);
            lAcc = lAcc + lDeltas[i];
            rAcc = rAcc + rDeltas[i];
        }

        // Batch oracle: for every left person, either all matching role rows
        // (inner) or a single NULL-padded row if no matches.
        var expected = new ZSetBuilder<OutRow, Z64>();
        var rightByKey = new Dictionary<int, ZSet<Role, Z64>>();
        foreach (var (r, w) in rAcc)
        {
            var k = r.PersonId;
            rightByKey[k] = (rightByKey.TryGetValue(k, out var g) ? g : ZSet<Role, Z64>.Empty)
                + ZSet.Singleton(r, w);
        }

        foreach (var (p, pw) in lAcc)
        {
            if (rightByKey.TryGetValue(p.Id, out var matches) && !matches.IsEmpty)
            {
                foreach (var (r, rw) in matches)
                {
                    expected.Add(new OutRow(p.Id, p.Name, r.RoleName), Z64.Multiply(pw, rw));
                }
            }
            else
            {
                expected.Add(new OutRow(p.Id, p.Name, null), pw);
            }
        }

        Assert.Equal(expected.Build(), outAcc);
    }
}
