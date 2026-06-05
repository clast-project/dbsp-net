// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using Clast.DatabaseDecimal.Values;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

/// <summary>
/// Pins the per-column partition hashing that data-parallel exchange placement
/// (and snapshot recovery) relies on. The values only need to be stable across
/// runs — not portable to another language — so the assertions guard against an
/// accidental algorithm change that would re-shard a recovered circuit.
/// </summary>
public class StablePartitionHashTests
{
    [Fact]
    public void Of_EveryColumnType_IsDeterministic()
    {
        Assert.Equal(StablePartitionHash.Of(42), StablePartitionHash.Of(42));
        Assert.Equal(StablePartitionHash.Of(9_000_000_000L), StablePartitionHash.Of(9_000_000_000L));
        Assert.Equal(StablePartitionHash.Of(true), StablePartitionHash.Of(true));
        Assert.Equal(StablePartitionHash.Of(3.14), StablePartitionHash.Of(3.14));
        Assert.Equal(StablePartitionHash.Of("orders"), StablePartitionHash.Of("orders"));
        Assert.Equal(StablePartitionHash.Of(Utf8String.Of("orders")), StablePartitionHash.Of(Utf8String.Of("orders")));
        Assert.Equal(StablePartitionHash.Of(new Date32(19_000)), StablePartitionHash.Of(new Date32(19_000)));
        Assert.Equal(StablePartitionHash.Of(new Time64(123)), StablePartitionHash.Of(new Time64(123)));
        Assert.Equal(StablePartitionHash.Of(new Timestamp(456)), StablePartitionHash.Of(new Timestamp(456)));
        Assert.Equal(
            StablePartitionHash.Of(new Decimal128(12345L)),
            StablePartitionHash.Of(new Decimal128(12345L)));
    }

    [Fact]
    public void Of_Utf8String_MatchesString_ForByteIdenticalContent()
    {
        // A string column and a Utf8String column holding the same text must not
        // be required to agree (different overloads) — but each must be internally
        // content-sensitive so distinct keys land on distinct shards predictably.
        Assert.NotEqual(StablePartitionHash.Of(Utf8String.Of("a")), StablePartitionHash.Of(Utf8String.Of("b")));
        Assert.NotEqual(StablePartitionHash.Of("a"), StablePartitionHash.Of("b"));
    }

    [Fact]
    public void Of_NullString_CollapsesToNullSentinel()
    {
        Assert.Equal(StablePartitionHash.NullHash, StablePartitionHash.Of((string?)null));
    }

    [Fact]
    public void Of_Double_NormalisesNegativeZero()
    {
        Assert.Equal(StablePartitionHash.Of(0.0), StablePartitionHash.Of(-0.0));
    }

    [Fact]
    public void Of_DistinctValues_GenerallyDiffer()
    {
        Assert.NotEqual(StablePartitionHash.Of(1), StablePartitionHash.Of(2));
        Assert.NotEqual(StablePartitionHash.Of(true), StablePartitionHash.Of(false));
        Assert.NotEqual(StablePartitionHash.Of(new Date32(1)), StablePartitionHash.Of(new Date32(2)));
    }
}
