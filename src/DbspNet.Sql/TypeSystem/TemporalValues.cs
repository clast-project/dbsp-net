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

    /// <summary>
    /// The day-number (days since epoch) of the calendar day containing the
    /// instant <paramref name="microsSinceEpoch"/> — i.e. <c>CURRENT_DATE</c> of
    /// a logical clock at that instant. This is the monotone clock-to-day
    /// transform a <c>CURRENT_DATE</c> temporal filter runs the µs clock through
    /// (see <c>docs/now-and-temporal-filters.md</c>). Returns a 64-bit
    /// day-number — the same value <see cref="Days"/> carries, widened — so it
    /// composes with the 64-bit frontier machinery; floor (not truncate toward
    /// zero) so pre-epoch instants map to the correct day.
    /// </summary>
    public static long DayNumberFloor(long microsSinceEpoch)
    {
        var q = microsSinceEpoch / Interval.MicrosPerDay;
        var r = microsSinceEpoch % Interval.MicrosPerDay;
        return r < 0 ? q - 1 : q;
    }

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

/// <summary>
/// The field range an <c>INTERVAL</c> literal / type was written with. The
/// single-field forms plus the two common compound forms; this also encodes
/// which of SQL's two interval classes a value belongs to (year-month vs
/// day-time) — see <see cref="IntervalQualifiers.IsYearMonth"/>.
/// </summary>
public enum IntervalQualifier
{
    Year,
    Month,
    YearToMonth,
    Day,
    Hour,
    Minute,
    Second,
    DayToSecond,
}

/// <summary>Helpers over <see cref="IntervalQualifier"/>.</summary>
public static class IntervalQualifiers
{
    /// <summary>True for the year-month class (Year / Month / Year-to-month).</summary>
    public static bool IsYearMonth(IntervalQualifier q) =>
        q is IntervalQualifier.Year or IntervalQualifier.Month or IntervalQualifier.YearToMonth;

    /// <summary>The SQL spelling of a qualifier, e.g. <c>YEAR TO MONTH</c>.</summary>
    public static string Display(IntervalQualifier q) => q switch
    {
        IntervalQualifier.Year => "YEAR",
        IntervalQualifier.Month => "MONTH",
        IntervalQualifier.YearToMonth => "YEAR TO MONTH",
        IntervalQualifier.Day => "DAY",
        IntervalQualifier.Hour => "HOUR",
        IntervalQualifier.Minute => "MINUTE",
        IntervalQualifier.Second => "SECOND",
        IntervalQualifier.DayToSecond => "DAY TO SECOND",
        _ => q.ToString(),
    };

    /// <summary>
    /// Parse a single field name (case-insensitive) into a single-field
    /// qualifier. Returns null for an unrecognised word so the parser can
    /// produce a clean error.
    /// </summary>
    public static IntervalQualifier? ParseField(string word) => word.ToLowerInvariant() switch
    {
        "year" => IntervalQualifier.Year,
        "month" => IntervalQualifier.Month,
        "day" => IntervalQualifier.Day,
        "hour" => IntervalQualifier.Hour,
        "minute" => IntervalQualifier.Minute,
        "second" => IntervalQualifier.Second,
        _ => null,
    };
}

/// <summary>
/// A SQL <c>INTERVAL</c> value. SQL has two incompatible interval families and
/// this struct carries one component for each: <see cref="Months"/> for the
/// year-month class (calendar months — their real length varies) and
/// <see cref="Micros"/> for the day-time class (a fixed microsecond count).
/// For any given value only the component matching its
/// <see cref="IntervalQualifier"/> class is non-zero. Layout mirrors Arrow's
/// <c>MonthDayNano</c> spirit (months separate from sub-day time).
/// </summary>
public readonly record struct Interval(int Months, long Micros)
    : IComparable<Interval>, IComparable
{
    public const long MicrosPerSecond = 1_000_000L;
    public const long MicrosPerMinute = 60L * MicrosPerSecond;
    public const long MicrosPerHour = 60L * MicrosPerMinute;
    public const long MicrosPerDay = 24L * MicrosPerHour;

    public static readonly Interval Zero = new(0, 0);

    /// <summary>
    /// Parse the textual magnitude of an interval literal under a qualifier —
    /// e.g. <c>("90", DAY)</c> → 90 days; <c>("1-6", YEAR TO MONTH)</c> →
    /// 18 months; <c>("1.5", SECOND)</c> → 1,500,000µs. A leading <c>-</c>
    /// negates the whole magnitude.
    /// </summary>
    public static Interval Parse(string s, IntervalQualifier q)
    {
        s = s.Trim();
        return q switch
        {
            IntervalQualifier.Year => new Interval(checked(ParseIntField(s) * 12), 0),
            IntervalQualifier.Month => new Interval(ParseIntField(s), 0),
            IntervalQualifier.YearToMonth => ParseYearToMonth(s),
            IntervalQualifier.Day => new Interval(0, ScaleToMicros(s, MicrosPerDay)),
            IntervalQualifier.Hour => new Interval(0, ScaleToMicros(s, MicrosPerHour)),
            IntervalQualifier.Minute => new Interval(0, ScaleToMicros(s, MicrosPerMinute)),
            IntervalQualifier.Second => new Interval(0, ScaleToMicros(s, MicrosPerSecond)),
            IntervalQualifier.DayToSecond => ParseDayToSecond(s),
            _ => throw new FormatException($"unsupported interval qualifier {q}"),
        };
    }

    private static int ParseIntField(string s) =>
        int.Parse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);

    private static long ScaleToMicros(string s, long unitMicros)
    {
        // Parse as decimal so fractional fields (e.g. '1.5' SECOND) are exact.
        var v = decimal.Parse(s, NumberStyles.Number | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        return (long)decimal.Round(v * unitMicros);
    }

    private static Interval ParseYearToMonth(string s)
    {
        var sign = 1;
        if (s.StartsWith('-')) { sign = -1; s = s[1..]; }
        else if (s.StartsWith('+')) { s = s[1..]; }

        var parts = s.Split('-');
        if (parts.Length != 2)
        {
            throw new FormatException($"INTERVAL YEAR TO MONTH expects 'years-months', got '{s}'");
        }

        var years = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var months = int.Parse(parts[1], CultureInfo.InvariantCulture);
        return new Interval(sign * checked(years * 12 + months), 0);
    }

    private static Interval ParseDayToSecond(string s)
    {
        var sign = 1;
        if (s.StartsWith('-')) { sign = -1; s = s[1..]; }
        else if (s.StartsWith('+')) { s = s[1..]; }

        // Forms: "d", "d h:m:s[.f]", or "h:m:s[.f]".
        long days = 0;
        var spaceIdx = s.IndexOf(' ');
        string timePart;
        if (spaceIdx >= 0)
        {
            days = long.Parse(s[..spaceIdx], CultureInfo.InvariantCulture);
            timePart = s[(spaceIdx + 1)..];
        }
        else if (s.Contains(':'))
        {
            timePart = s;
        }
        else
        {
            // Just a day count.
            return new Interval(0, sign * long.Parse(s, CultureInfo.InvariantCulture) * MicrosPerDay);
        }

        var t = timePart.Split(':');
        if (t.Length is < 2 or > 3)
        {
            throw new FormatException($"INTERVAL DAY TO SECOND time part expects 'h:m[:s]', got '{timePart}'");
        }

        var hours = long.Parse(t[0], CultureInfo.InvariantCulture);
        var minutes = long.Parse(t[1], CultureInfo.InvariantCulture);
        var secondsMicros = t.Length == 3
            ? (long)decimal.Round(decimal.Parse(t[2], CultureInfo.InvariantCulture) * MicrosPerSecond)
            : 0;
        var micros = days * MicrosPerDay + hours * MicrosPerHour + minutes * MicrosPerMinute + secondsMicros;
        return new Interval(0, sign * micros);
    }

    public int CompareTo(Interval other)
    {
        var c = Months.CompareTo(other.Months);
        return c != 0 ? c : Micros.CompareTo(other.Micros);
    }

    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        Interval i => CompareTo(i),
        _ => throw new ArgumentException($"object must be of type {nameof(Interval)}"),
    };

    public static bool operator <(Interval a, Interval b) => a.CompareTo(b) < 0;
    public static bool operator <=(Interval a, Interval b) => a.CompareTo(b) <= 0;
    public static bool operator >(Interval a, Interval b) => a.CompareTo(b) > 0;
    public static bool operator >=(Interval a, Interval b) => a.CompareTo(b) >= 0;

    /// <summary>
    /// Canonical ISO-8601 duration spelling (e.g. <c>P1Y2M</c>, <c>P90D</c>,
    /// <c>PT1H30M</c>), used for <c>CAST(interval AS VARCHAR)</c> and
    /// diagnostics. Qualifier-independent — derived purely from the stored
    /// month / microsecond magnitudes.
    /// </summary>
    public override string ToString()
    {
        if (Months == 0 && Micros == 0)
        {
            return "PT0S";
        }

        var sb = new System.Text.StringBuilder("P");
        if (Months != 0)
        {
            var years = Months / 12;
            var months = Months % 12;
            if (years != 0) sb.Append(years).Append('Y');
            if (months != 0) sb.Append(months).Append('M');
        }

        var micros = Micros;
        var days = micros / MicrosPerDay;
        micros %= MicrosPerDay;
        if (days != 0) sb.Append(days).Append('D');

        if (micros != 0)
        {
            sb.Append('T');
            var hours = micros / MicrosPerHour;
            micros %= MicrosPerHour;
            var minutes = micros / MicrosPerMinute;
            micros %= MicrosPerMinute;
            if (hours != 0) sb.Append(hours).Append('H');
            if (minutes != 0) sb.Append(minutes).Append('M');
            if (micros != 0)
            {
                var seconds = micros / (decimal)MicrosPerSecond;
                sb.Append(seconds.ToString(CultureInfo.InvariantCulture)).Append('S');
            }
        }

        return sb.ToString();
    }
}
