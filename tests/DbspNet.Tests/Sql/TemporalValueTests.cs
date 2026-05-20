// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Sql;

public class TemporalValueTests
{
    // ---- Date32 ----

    [Fact]
    public void Date32_EpochIsZero()
    {
        Assert.Equal(0, Date32.Epoch.Days);
        Assert.Equal(new DateOnly(1970, 1, 1), Date32.Epoch.ToDateOnly());
    }

    [Fact]
    public void Date32_RoundTripsThroughDateOnly()
    {
        var d = new DateOnly(2026, 4, 27);
        var d32 = Date32.FromDateOnly(d);
        Assert.Equal(d, d32.ToDateOnly());
    }

    [Fact]
    public void Date32_ParsesIso8601()
    {
        var parsed = Date32.Parse("2026-04-27");
        Assert.Equal(new DateOnly(2026, 4, 27), parsed.ToDateOnly());
    }

    [Fact]
    public void Date32_ToStringIsIso8601()
    {
        var d = Date32.FromDateOnly(new DateOnly(2026, 4, 27));
        Assert.Equal("2026-04-27", d.ToString());
    }

    [Fact]
    public void Date32_OrdersChronologically()
    {
        var a = Date32.Parse("2026-04-26");
        var b = Date32.Parse("2026-04-27");
        Assert.True(a < b);
        Assert.True(a.CompareTo(b) < 0);
    }

    [Fact]
    public void Date32_PreEpochNegative()
    {
        var d = Date32.Parse("1969-12-31");
        Assert.Equal(-1, d.Days);
    }

    // ---- Time64 ----

    [Fact]
    public void Time64_MidnightIsZero()
    {
        Assert.Equal(0, Time64.Midnight.Microseconds);
    }

    [Fact]
    public void Time64_RoundTripsThroughTimeOnly()
    {
        var t = new TimeOnly(14, 30, 15, 250).Add(TimeSpan.FromTicks(7));
        var t64 = Time64.FromTimeOnly(t);
        // Microsecond resolution drops sub-microsecond ticks.
        Assert.Equal(t.Ticks / 10 * 10, t64.ToTimeOnly().Ticks);
    }

    [Fact]
    public void Time64_ParsesWithAndWithoutFraction()
    {
        Assert.Equal(0, Time64.Parse("00:00:00").Microseconds);
        Assert.Equal(3_600_000_000L, Time64.Parse("01:00:00").Microseconds);
        Assert.Equal(3_600_500_000L, Time64.Parse("01:00:00.5").Microseconds);
        Assert.Equal(3_600_000_001L, Time64.Parse("01:00:00.000001").Microseconds);
    }

    [Fact]
    public void Time64_OrdersByMicroseconds()
    {
        var a = Time64.Parse("09:00:00");
        var b = Time64.Parse("17:30:00");
        Assert.True(a < b);
    }

    // ---- Timestamp ----

    [Fact]
    public void Timestamp_EpochIsZero()
    {
        Assert.Equal(0, Timestamp.Epoch.Microseconds);
        Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified),
            Timestamp.Epoch.ToDateTime());
    }

    [Fact]
    public void Timestamp_RoundTripsThroughDateTime()
    {
        var dt = new DateTime(2026, 4, 27, 14, 30, 15, DateTimeKind.Unspecified);
        var ts = Timestamp.FromDateTime(dt);
        Assert.Equal(dt, ts.ToDateTime());
    }

    [Fact]
    public void Timestamp_ParsesIsoSpaceAndT()
    {
        var withSpace = Timestamp.Parse("2026-04-27 14:30:15");
        var withT = Timestamp.Parse("2026-04-27T14:30:15");
        Assert.Equal(withSpace, withT);
    }

    [Fact]
    public void Timestamp_ParsesFractionalSeconds()
    {
        var ts = Timestamp.Parse("2026-04-27 00:00:00.123456");
        // 123_456 microseconds past 2026-04-27 00:00 UTC
        var baseTs = Timestamp.Parse("2026-04-27 00:00:00");
        Assert.Equal(123_456, ts.Microseconds - baseTs.Microseconds);
    }

    [Fact]
    public void Timestamp_OrdersChronologically()
    {
        var a = Timestamp.Parse("2026-04-27 09:00:00");
        var b = Timestamp.Parse("2026-04-27 17:30:00");
        Assert.True(a < b);
    }

    // ---- Equality / hashing (used by codecs and Z-set keys) ----

    [Fact]
    public void Date32_EqualValuesShareHash()
    {
        var a = Date32.Parse("2026-04-27");
        var b = Date32.Parse("2026-04-27");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Time64_EqualValuesShareHash()
    {
        var a = Time64.Parse("12:00:00");
        var b = Time64.Parse("12:00:00.000000");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Timestamp_EqualValuesShareHash()
    {
        var a = Timestamp.Parse("2026-04-27 12:00:00");
        var b = Timestamp.Parse("2026-04-27T12:00:00");
        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }
}
