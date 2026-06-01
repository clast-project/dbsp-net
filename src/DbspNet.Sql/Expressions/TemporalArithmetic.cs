// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// Thin, deterministic helpers invoked from the compiled Expression trees of
/// both the structural <see cref="ExpressionCompiler"/> and the typed
/// <see cref="TypedExpressionCompiler"/> for date/time/interval arithmetic.
/// Every method is a pure function of its operands — required for incremental
/// correctness (a relational map/filter is linear in the Z-set for any pure
/// scalar function).
/// </summary>
/// <remarks>
/// Month addition is calendar-aware (via <see cref="DateOnly.AddMonths"/> /
/// <see cref="DateTime.AddMonths"/>) so e.g. <c>Jan-31 + 1 month = Feb-28</c>.
/// DATE arithmetic is day-granular: a day-time interval shifts a DATE by whole
/// days (sub-day microseconds are truncated toward zero), keeping DATE closed
/// under <c>± interval</c> rather than promoting to TIMESTAMP.
/// </remarks>
internal static class TemporalArithmetic
{
    public static Date32 AddToDate(Date32 d, Interval iv)
    {
        var date = d.ToDateOnly();
        if (iv.Months != 0)
        {
            date = date.AddMonths(iv.Months);
        }

        if (iv.Micros != 0)
        {
            date = date.AddDays((int)(iv.Micros / Interval.MicrosPerDay));
        }

        return Date32.FromDateOnly(date);
    }

    public static Date32 SubFromDate(Date32 d, Interval iv) => AddToDate(d, Negate(iv));

    public static Timestamp AddToTimestamp(Timestamp t, Interval iv)
    {
        var dt = t.ToDateTime();
        if (iv.Months != 0)
        {
            dt = dt.AddMonths(iv.Months);
        }

        if (iv.Micros != 0)
        {
            dt = dt.AddTicks(iv.Micros * 10);
        }

        return Timestamp.FromDateTime(dt);
    }

    public static Timestamp SubFromTimestamp(Timestamp t, Interval iv) => AddToTimestamp(t, Negate(iv));

    public static Time64 AddToTime(Time64 t, Interval iv)
    {
        // Year-month intervals on TIME are rejected at resolve time; only the
        // microsecond component applies, wrapped into [0, 24h).
        var day = Time64.MicrosecondsPerDay;
        var micros = (t.Microseconds + iv.Micros) % day;
        if (micros < 0)
        {
            micros += day;
        }

        return new Time64(micros);
    }

    public static Time64 SubFromTime(Time64 t, Interval iv) => AddToTime(t, Negate(iv));

    public static Interval AddIntervals(Interval a, Interval b) =>
        new(checked(a.Months + b.Months), checked(a.Micros + b.Micros));

    public static Interval SubIntervals(Interval a, Interval b) =>
        new(checked(a.Months - b.Months), checked(a.Micros - b.Micros));

    public static Interval Negate(Interval a) => new(-a.Months, -a.Micros);

    public static Interval MulInterval(Interval a, double k) =>
        new((int)Math.Round(a.Months * k), (long)Math.Round(a.Micros * k));

    public static Interval DivInterval(Interval a, double k) =>
        new((int)Math.Round(a.Months / k), (long)Math.Round(a.Micros / k));

    public static Interval DiffDates(Date32 a, Date32 b) =>
        new(0, (long)(a.Days - b.Days) * Interval.MicrosPerDay);

    public static Interval DiffTimestamps(Timestamp a, Timestamp b) =>
        new(0, a.Microseconds - b.Microseconds);

    public static Interval DiffTimes(Time64 a, Time64 b) =>
        new(0, a.Microseconds - b.Microseconds);
}
