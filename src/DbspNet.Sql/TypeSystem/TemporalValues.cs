// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Globalization;

namespace DbspNet.Sql.TypeSystem;

/// <summary>
/// Arrow-aligned date value: int32 days since the UNIX epoch (1970-01-01).
/// Layout matches Arrow's <c>Date32</c> type so the underlying buffer can
/// round-trip without conversion. Naive (no timezone).
/// </summary>
public readonly record struct Date32(int Days) : IComparable<Date32>, IComparable
{
    private const int UnixEpochDayNumber = 719_162;

    public static readonly Date32 Epoch = new(0);

    public static Date32 FromDateOnly(DateOnly date) =>
        new(date.DayNumber - UnixEpochDayNumber);

    public DateOnly ToDateOnly() => DateOnly.FromDayNumber(Days + UnixEpochDayNumber);

    public static Date32 Parse(string s) =>
        FromDateOnly(DateOnly.ParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture));

    public int CompareTo(Date32 other) => Days.CompareTo(other.Days);

    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        Date32 d => CompareTo(d),
        _ => throw new ArgumentException($"object must be of type {nameof(Date32)}"),
    };

    public static bool operator <(Date32 a, Date32 b) => a.Days < b.Days;
    public static bool operator <=(Date32 a, Date32 b) => a.Days <= b.Days;
    public static bool operator >(Date32 a, Date32 b) => a.Days > b.Days;
    public static bool operator >=(Date32 a, Date32 b) => a.Days >= b.Days;

    public override string ToString() =>
        ToDateOnly().ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
}

/// <summary>
/// Arrow-aligned time-of-day value: int64 microseconds since midnight. Range
/// is [0, 86_400_000_000). Layout matches Arrow's <c>Time64[microsecond]</c>.
/// </summary>
public readonly record struct Time64(long Microseconds) : IComparable<Time64>, IComparable
{
    private const long MicrosPerDay = 86_400L * 1_000_000L;

    public static readonly Time64 Midnight = new(0);

    public static Time64 FromTimeOnly(TimeOnly time) => new(time.Ticks / 10);

    public TimeOnly ToTimeOnly() => new(Microseconds * 10);

    public static Time64 Parse(string s)
    {
        var formats = new[]
        {
            "HH:mm:ss",
            "HH:mm:ss.f",
            "HH:mm:ss.ff",
            "HH:mm:ss.fff",
            "HH:mm:ss.ffff",
            "HH:mm:ss.fffff",
            "HH:mm:ss.ffffff",
        };
        var time = TimeOnly.ParseExact(s, formats, CultureInfo.InvariantCulture);
        return FromTimeOnly(time);
    }

    public int CompareTo(Time64 other) => Microseconds.CompareTo(other.Microseconds);

    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        Time64 t => CompareTo(t),
        _ => throw new ArgumentException($"object must be of type {nameof(Time64)}"),
    };

    public static bool operator <(Time64 a, Time64 b) => a.Microseconds < b.Microseconds;
    public static bool operator <=(Time64 a, Time64 b) => a.Microseconds <= b.Microseconds;
    public static bool operator >(Time64 a, Time64 b) => a.Microseconds > b.Microseconds;
    public static bool operator >=(Time64 a, Time64 b) => a.Microseconds >= b.Microseconds;

    public override string ToString() =>
        ToTimeOnly().ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    /// <summary>Microseconds in a full day (86,400,000,000) for range checks.</summary>
    public static long MicrosecondsPerDay => MicrosPerDay;
}

/// <summary>
/// Arrow-aligned naive timestamp: int64 microseconds since the UNIX epoch
/// (1970-01-01 00:00:00). No timezone — wall-clock semantics; layout matches
/// Arrow's <c>Timestamp[microsecond]</c> with <c>tz=null</c>.
/// </summary>
public readonly record struct Timestamp(long Microseconds) : IComparable<Timestamp>, IComparable
{
    private const long UnixEpochTicks = 621_355_968_000_000_000L;

    public static readonly Timestamp Epoch = new(0);

    public static Timestamp FromDateTime(DateTime dt) =>
        new((dt.Ticks - UnixEpochTicks) / 10);

    public DateTime ToDateTime() =>
        new(Microseconds * 10 + UnixEpochTicks, DateTimeKind.Unspecified);

    public static Timestamp Parse(string s)
    {
        var formats = new[]
        {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.f",
            "yyyy-MM-dd HH:mm:ss.ff",
            "yyyy-MM-dd HH:mm:ss.fff",
            "yyyy-MM-dd HH:mm:ss.ffff",
            "yyyy-MM-dd HH:mm:ss.fffff",
            "yyyy-MM-dd HH:mm:ss.ffffff",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy-MM-ddTHH:mm:ss.f",
            "yyyy-MM-ddTHH:mm:ss.ff",
            "yyyy-MM-ddTHH:mm:ss.fff",
            "yyyy-MM-ddTHH:mm:ss.ffff",
            "yyyy-MM-ddTHH:mm:ss.fffff",
            "yyyy-MM-ddTHH:mm:ss.ffffff",
        };
        var dt = DateTime.ParseExact(
            s, formats, CultureInfo.InvariantCulture,
            DateTimeStyles.None);
        return FromDateTime(dt);
    }

    public int CompareTo(Timestamp other) => Microseconds.CompareTo(other.Microseconds);

    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        Timestamp t => CompareTo(t),
        _ => throw new ArgumentException($"object must be of type {nameof(Timestamp)}"),
    };

    public static bool operator <(Timestamp a, Timestamp b) => a.Microseconds < b.Microseconds;
    public static bool operator <=(Timestamp a, Timestamp b) => a.Microseconds <= b.Microseconds;
    public static bool operator >(Timestamp a, Timestamp b) => a.Microseconds > b.Microseconds;
    public static bool operator >=(Timestamp a, Timestamp b) => a.Microseconds >= b.Microseconds;

    public override string ToString() =>
        ToDateTime().ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
}
