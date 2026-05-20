// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Core.Operators.Linear;
using DbspNet.Core.Operators.Stateful;
using DbspNet.Core.Operators.Stateful.Spine;

namespace DbspNet.Tests.Operators.Stateful.Spine;

public class SpineIncrementalJoinOpTests
{
    private sealed record Person(int Id, string Name) : IComparable<Person>
    {
        public int CompareTo(Person? other) =>
            other is null ? 1
            : Id != other.Id ? Id.CompareTo(other.Id)
            : string.CompareOrdinal(Name, other.Name);
    }

    private sealed record Role(int PersonId, string RoleName) : IComparable<Role>
    {
        public int CompareTo(Role? other) =>
            other is null ? 1
            : PersonId != other.PersonId ? PersonId.CompareTo(other.PersonId)
            : string.CompareOrdinal(RoleName, other.RoleName);
    }

    private static (
        RootCircuit Circuit,
        InputHandle<ZSet<Person, Z64>> Left,
        InputHandle<ZSet<Role, Z64>> Right,
        OutputHandle<ZSet<(int Id, string Name, string RoleName), Z64>> Output)
        BuildSpineJoinCircuit(ICompactionStrategy? strategy = null)
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
            oh = b.Output(b.SpineIncrementalInnerJoin(
                lIdx, rIdx,
                (_, p, r) => (p.Id, p.Name, r.RoleName),
                compactionStrategy: strategy));
        });
        return (c, li!, ri!, oh!);
    }

    [Fact]
    public void BothSidesInSameTick_EmitsCrossJoinOfDeltas()
    {
        var (c, left, right, oh) = BuildSpineJoinCircuit();
        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();

        Assert.Equal(1, oh.Current.Count);
        Assert.Equal(Z64.One, oh.Current.WeightOf((1, "alice", "admin")));
    }

    [Fact]
    public void LateArrivalOnRightSide_EmitsHistoricalMatches()
    {
        var (c, left, right, oh) = BuildSpineJoinCircuit();

        left.Push(ZSet.Singleton(new Person(1, "alice"), Z64.One));
        c.Step();
        Assert.True(oh.Current.IsEmpty);

        right.Push(ZSet.Singleton(new Role(1, "admin"), Z64.One));
        c.Step();
        Assert.Equal(Z64.One, oh.Current.WeightOf((1, "alice", "admin")));
    }

    [Fact]
    public void RetractionOfPerson_EmitsMatchingRowRetractions()
    {
        var (c, left, right, oh) = BuildSpineJoinCircuit();

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

        Assert.Equal(new Z64(-1), oh.Current.WeightOf((1, "alice", "admin")));
        Assert.Equal(new Z64(-1), oh.Current.WeightOf((1, "alice", "reader")));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    public void MatchesFlatJoinAcrossCompactionThresholds(int batchesPerLevel)
    {
        var rng = new Random(Seed: 53 + batchesPerLevel);

        InputHandle<ZSet<Person, Z64>>? flatL = null;
        InputHandle<ZSet<Role, Z64>>? flatR = null;
        OutputHandle<ZSet<(int Id, string Name, string RoleName), Z64>>? flatOut = null;
        var flatCircuit = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<Person, Z64>();
            var (rh, rs) = b.ZSetInput<Role, Z64>();
            flatL = lh;
            flatR = rh;
            var lIdx = b.IndexBy(ls, p => p.Id);
            var rIdx = b.IndexBy(rs, r => r.PersonId);
            flatOut = b.Output(b.IncrementalInnerJoin(lIdx, rIdx, (_, p, r) => (p.Id, p.Name, r.RoleName)));
        });

        InputHandle<ZSet<Person, Z64>>? spineL = null;
        InputHandle<ZSet<Role, Z64>>? spineR = null;
        OutputHandle<ZSet<(int Id, string Name, string RoleName), Z64>>? spineOut = null;
        var spineCircuit = RootCircuit.Build(b =>
        {
            var (lh, ls) = b.ZSetInput<Person, Z64>();
            var (rh, rs) = b.ZSetInput<Role, Z64>();
            spineL = lh;
            spineR = rh;
            var lIdx = b.IndexBy(ls, p => p.Id);
            var rIdx = b.IndexBy(rs, r => r.PersonId);
            spineOut = b.Output(b.SpineIncrementalInnerJoin(
                lIdx, rIdx,
                (_, p, r) => (p.Id, p.Name, r.RoleName),
                compactionStrategy: new TieredCompactionStrategy(batchesPerLevel)));
        });

        for (var step = 0; step < 150; step++)
        {
            var ld = RandomPersonDelta(rng, keySpace: 8, maxEntries: 3);
            var rd = RandomRoleDelta(rng, keySpace: 8, maxEntries: 3);
            flatL!.Push(ld);
            flatR!.Push(rd);
            spineL!.Push(ld);
            spineR!.Push(rd);
            flatCircuit.Step();
            spineCircuit.Step();

            Assert.Equal(flatOut!.Current, spineOut!.Current);
        }
    }

    private static ZSet<Person, Z64> RandomPersonDelta(Random rng, int keySpace, int maxEntries)
    {
        var n = rng.Next(maxEntries + 1);
        if (n == 0) return ZSet<Person, Z64>.Empty;
        var b = new ZSetBuilder<Person, Z64>();
        for (var i = 0; i < n; i++)
        {
            var id = rng.Next(keySpace);
            var w = rng.Next(-2, 3);
            if (w == 0) continue;
            b.Add(new Person(id, $"p{id}"), new Z64(w));
        }
        return b.Build();
    }

    private static ZSet<Role, Z64> RandomRoleDelta(Random rng, int keySpace, int maxEntries)
    {
        var n = rng.Next(maxEntries + 1);
        if (n == 0) return ZSet<Role, Z64>.Empty;
        var b = new ZSetBuilder<Role, Z64>();
        var roles = new[] { "admin", "reader", "author" };
        for (var i = 0; i < n; i++)
        {
            var id = rng.Next(keySpace);
            var role = roles[rng.Next(roles.Length)];
            var w = rng.Next(-2, 3);
            if (w == 0) continue;
            b.Add(new Role(id, role), new Z64(w));
        }
        return b.Build();
    }
}
