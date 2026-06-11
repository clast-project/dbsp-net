// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Sql.Expressions;

/// <summary>
/// The temporal field / truncation unit shared by EXTRACT, DATE_TRUNC,
/// DATEADD, and DATEDIFF. Not every member is valid in every function (e.g.
/// <see cref="Dow"/> / <see cref="Doy"/> / <see cref="Epoch"/> are EXTRACT-only)
/// — each function validates the subset it accepts at resolve time.
/// </summary>
internal enum TemporalField
{
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Dow,
    Doy,
    Hour,
    Minute,
    Second,
    Epoch,
}

internal static class TemporalFieldNames
{
    public static TemporalField? Parse(string s) => s.Trim().ToLowerInvariant() switch
    {
        "year" or "years" or "yyyy" or "yy" => TemporalField.Year,
        "quarter" or "qtr" or "qq" or "q" => TemporalField.Quarter,
        "month" or "months" or "mon" or "mm" or "m" => TemporalField.Month,
        "week" or "weeks" or "wk" or "ww" => TemporalField.Week,
        "day" or "days" or "dd" or "d" => TemporalField.Day,
        "dow" or "dayofweek" or "weekday" => TemporalField.Dow,
        "doy" or "dayofyear" => TemporalField.Doy,
        "hour" or "hours" or "hh" => TemporalField.Hour,
        "minute" or "minutes" or "mi" or "n" => TemporalField.Minute,
        "second" or "seconds" or "ss" or "s" => TemporalField.Second,
        "epoch" => TemporalField.Epoch,
        _ => null,
    };
}

// ---------------------------------------------------------------------------
// Registry entries
// ---------------------------------------------------------------------------

/// <summary>
/// <c>EXTRACT(field FROM source)</c> / <c>DATE_PART('field', source)</c> — pull
/// one calendar/clock field out of a temporal value. The field is a constant
/// (the parser lowers the EXTRACT keyword form to a string-literal first arg),
/// so it's validated and baked in at resolve time. Integer fields return
/// BIGINT; SECOND / EPOCH return DOUBLE (fractional). Typed path falls back to
/// structural (temporal functions aren't on the typed fast path).
/// </summary>
internal sealed class ExtractFunction : IScalarFunction
{
    public string Name => "extract";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 2)
        {
            throw new ResolveException("EXTRACT takes a field and a source (EXTRACT(field FROM source))");
        }

        var field = TemporalFunctionSupport.ConstField(args, "EXTRACT");
        TemporalFunctionSupport.RequireTemporal("EXTRACT", args[1]);

        var valid = args[1].Type switch
        {
            SqlDateType => field is TemporalField.Year or TemporalField.Quarter or TemporalField.Month
                or TemporalField.Week or TemporalField.Day or TemporalField.Dow or TemporalField.Doy
                or TemporalField.Epoch,
            SqlTimeType => field is TemporalField.Hour or TemporalField.Minute or TemporalField.Second
                or TemporalField.Epoch,
            SqlTimestampType => true,
            _ => false,
        };
        if (!valid)
        {
            throw new ResolveException($"EXTRACT field {field} is not valid for {args[1].Type.Display}");
        }

        var nullable = args[1].Type.Nullable;
        SqlType result = field is TemporalField.Second or TemporalField.Epoch
            ? new SqlDoubleType(nullable)
            : new SqlBigintType(nullable);
        return new ResolvedFunctionCall(Name, args, result);
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var field = TemporalFunctionSupport.ConstField(fn.Arguments, "EXTRACT");
        return Expression.Call(
            TemporalFunctionSupport.Method(nameof(TemporalFunctions.Extract)),
            buildArg(fn.Arguments[1]),
            Expression.Constant(field));
    }

    public Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) => null;
}

/// <summary>
/// <c>DATE_TRUNC('unit', source)</c> — round a temporal value down to the start
/// of the given unit. Returns the same temporal type as the source.
/// </summary>
internal sealed class DateTruncFunction : IScalarFunction
{
    public string Name => "date_trunc";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 2)
        {
            throw new ResolveException("DATE_TRUNC takes a unit and a source");
        }

        var unit = TemporalFunctionSupport.ConstField(args, "DATE_TRUNC");
        TemporalFunctionSupport.RequireTemporal("DATE_TRUNC", args[1]);

        var valid = args[1].Type switch
        {
            SqlDateType => unit is TemporalField.Year or TemporalField.Quarter or TemporalField.Month
                or TemporalField.Week or TemporalField.Day,
            SqlTimeType => unit is TemporalField.Hour or TemporalField.Minute or TemporalField.Second,
            SqlTimestampType => unit is TemporalField.Year or TemporalField.Quarter or TemporalField.Month
                or TemporalField.Week or TemporalField.Day or TemporalField.Hour or TemporalField.Minute
                or TemporalField.Second,
            _ => false,
        };
        if (!valid)
        {
            throw new ResolveException($"DATE_TRUNC unit {unit} is not valid for {args[1].Type.Display}");
        }

        return new ResolvedFunctionCall(Name, args, args[1].Type);
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var unit = TemporalFunctionSupport.ConstField(fn.Arguments, "DATE_TRUNC");
        return Expression.Call(
            TemporalFunctionSupport.Method(nameof(TemporalFunctions.DateTrunc)),
            buildArg(fn.Arguments[1]),
            Expression.Constant(unit));
    }

    public Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) => null;

    // date_trunc is non-decreasing in its source, but lowers values (truncates),
    // so the frontier must pass through the same truncation to threshold the
    // derived keys soundly.
    public ScalarMonotonicity? Monotonicity(ResolvedFunctionCall fn)
    {
        var unit = TemporalFunctionSupport.ConstField(fn.Arguments, "DATE_TRUNC");
        var src = fn.Arguments[1].Type;
        return new ScalarMonotonicity(1, v => TemporalFunctions.DateTruncFrontier(v, src, unit));
    }
}

/// <summary>
/// <c>tumble_start(timecol, INTERVAL size)</c> — the internal window-assignment
/// function the event-time TUMBLE windowing surface lowers onto: floor a temporal
/// value to the start of its fixed-size, non-overlapping window. Like
/// <see cref="DateTruncFunction"/> but bucketing by an arbitrary constant day-time
/// interval rather than a named calendar unit; returns the source temporal type.
/// </summary>
/// <remarks>
/// The parser desugars <c>TUMBLE(t, s)</c> and <c>TUMBLE_START(t, s)</c> to this
/// call and <c>TUMBLE_END(t, s)</c> to <c>tumble_start(t, s) + s</c>. A query that
/// <c>GROUP</c>s <c>BY TUMBLE(t, s)</c> and <c>SELECT</c>s <c>TUMBLE_START(t, s)</c> /
/// <c>TUMBLE_END(t, s)</c> therefore matches the group key through the resolver's
/// AstEqual post-aggregate machinery — so tumbling windows (Nexmark q7/q8/q12)
/// need no new plan node or operator. The bucket-floor monotonicity makes a
/// <c>GROUP BY</c> on the window start GC-able under LATENESS / clock watermarks:
/// a window is dropped only once the frontier passes <c>start + size</c>.
/// </remarks>
internal sealed class WindowStartFunction : IScalarFunction
{
    public string Name => "tumble_start";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 2)
        {
            throw new ResolveException("TUMBLE takes a time column and a window-size INTERVAL");
        }

        if (args[0].Type is not (SqlTimestampType or SqlDateType))
        {
            throw new ResolveException(
                $"TUMBLE requires a TIMESTAMP or DATE time column, got {args[0].Type.Display}");
        }

        // Validate the size now so a bad bucket is rejected at resolve time (it is
        // re-read identically by BuildStructural and Monotonicity).
        _ = WindowSizeNative(args, args[0].Type);
        return new ResolvedFunctionCall(Name, args, args[0].Type);
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var size = WindowSizeNative(fn.Arguments, fn.Arguments[0].Type);
        return Expression.Call(
            TemporalFunctionSupport.Method(nameof(TemporalFunctions.TumbleStart)),
            buildArg(fn.Arguments[0]),
            Expression.Constant(size));
    }

    public Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) =>
        TypedBuiltinScalarFunctions.BuildTumbleStart(
            typedArgs[0], fn.Arguments[0].Type, WindowSizeNative(fn.Arguments, fn.Arguments[0].Type));

    // Non-decreasing in the time column but floors values to the window start, so
    // — like date_trunc — the frontier must pass through the same bucket floor to
    // threshold the derived window-start keys soundly (a window may not be GC'd
    // until the watermark has passed its end, i.e. maxSeen >= start + size).
    public ScalarMonotonicity? Monotonicity(ResolvedFunctionCall fn)
    {
        var size = WindowSizeNative(fn.Arguments, fn.Arguments[0].Type);
        return new ScalarMonotonicity(0, v => TemporalFunctions.FloorToBucket(v, size));
    }

    /// <summary>The window size in the time column's native frontier unit
    /// (microseconds for TIMESTAMP, whole days for DATE). Rejects a non-constant
    /// size, a calendar (month/year) interval — whose bucket length is non-uniform,
    /// so its floor is not a monotone fixed-bucket map — a non-positive size, and a
    /// sub-day size over a DATE column.</summary>
    private static long WindowSizeNative(IReadOnlyList<ResolvedExpression> args, SqlType timeType)
    {
        if (args.Count < 2 || args[1] is not ResolvedLiteral { Value: Interval iv })
        {
            throw new ResolveException("TUMBLE window size must be a constant INTERVAL");
        }

        if (iv.Months != 0)
        {
            throw new ResolveException(
                "TUMBLE window size must be a day-time INTERVAL (a calendar month/year bucket is non-uniform)");
        }

        if (iv.Micros <= 0)
        {
            throw new ResolveException("TUMBLE window size must be a positive INTERVAL");
        }

        if (timeType is SqlDateType)
        {
            if (iv.Micros % Interval.MicrosPerDay != 0)
            {
                throw new ResolveException("a TUMBLE over a DATE column requires a whole-day window size");
            }

            return iv.Micros / Interval.MicrosPerDay;
        }

        return iv.Micros;
    }
}

/// <summary>
/// <c>DATEADD('unit', n, source)</c> — add <c>n</c> units to a temporal value.
/// Equivalent to <c>source + n * INTERVAL unit</c>; returns the source type.
/// (The unit is a string literal here, not SQL Server's bare keyword, to avoid
/// reserving the unit words.)
/// </summary>
internal sealed class DateAddFunction : IScalarFunction
{
    public string Name => "dateadd";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 3)
        {
            throw new ResolveException("DATEADD takes a unit, a count, and a source");
        }

        var unit = TemporalFunctionSupport.ConstField(args, "DATEADD");
        TemporalFunctionSupport.RequireDurationUnit(unit, "DATEADD");
        if (args[1].Type is not (SqlIntegerType or SqlBigintType))
        {
            throw new ResolveException("DATEADD count must be an integer");
        }

        TemporalFunctionSupport.RequireTemporal("DATEADD", args[2]);
        if (args[2].Type is SqlTimeType && !TemporalFunctionSupport.IsTimeUnit(unit))
        {
            throw new ResolveException($"DATEADD unit {unit} is not valid for TIME");
        }

        var nullable = args[1].Type.Nullable || args[2].Type.Nullable;
        return new ResolvedFunctionCall(Name, args, args[2].Type.WithNullable(nullable));
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var unit = TemporalFunctionSupport.ConstField(fn.Arguments, "DATEADD");
        return Expression.Call(
            TemporalFunctionSupport.Method(nameof(TemporalFunctions.DateAdd)),
            buildArg(fn.Arguments[2]),
            Expression.Constant(unit),
            buildArg(fn.Arguments[1]));
    }

    public Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) => null;

    // dateadd(unit, n, source) = source + n units. Monotone non-decreasing in
    // source when n is a non-negative constant (a forward shift), and a forward
    // shift keeps the raw frontier as a sound (conservative) threshold ⇒ identity
    // transform. Non-constant or negative n: can't prove it, so no GC.
    public ScalarMonotonicity? Monotonicity(ResolvedFunctionCall fn) =>
        fn.Arguments.Count == 3 && fn.Arguments[1] is ResolvedLiteral lit && IsNonNegativeInteger(lit.Value)
            ? new ScalarMonotonicity(2, null)
            : null;

    private static bool IsNonNegativeInteger(object? v) => v switch
    {
        int i => i >= 0,
        long l => l >= 0,
        _ => false,
    };
}

/// <summary>
/// <c>DATEDIFF('unit', start, end)</c> — number of <c>unit</c> boundaries
/// between two temporal values (<c>end − start</c>), as BIGINT. Boundary
/// semantics match SQL Server (e.g. <c>DATEDIFF('year', 2019-12-31,
/// 2020-01-01) = 1</c>); week boundaries are Monday-aligned.
/// </summary>
internal sealed class DateDiffFunction : IScalarFunction
{
    public string Name => "datediff";

    public ResolvedFunctionCall Resolve(IReadOnlyList<ResolvedExpression> args)
    {
        if (args.Count != 3)
        {
            throw new ResolveException("DATEDIFF takes a unit, a start, and an end");
        }

        var unit = TemporalFunctionSupport.ConstField(args, "DATEDIFF");
        TemporalFunctionSupport.RequireDurationUnit(unit, "DATEDIFF");
        TemporalFunctionSupport.RequireTemporal("DATEDIFF", args[1]);
        TemporalFunctionSupport.RequireTemporal("DATEDIFF", args[2]);
        if (args[1].Type.GetType() != args[2].Type.GetType())
        {
            throw new ResolveException(
                $"DATEDIFF start and end must be the same temporal kind ({args[1].Type.Display} vs {args[2].Type.Display})");
        }

        var nullable = args[1].Type.Nullable || args[2].Type.Nullable;
        return new ResolvedFunctionCall(Name, args, new SqlBigintType(nullable));
    }

    public Expression BuildStructural(ResolvedFunctionCall fn, Func<ResolvedExpression, Expression> buildArg)
    {
        var unit = TemporalFunctionSupport.ConstField(fn.Arguments, "DATEDIFF");
        return Expression.Call(
            TemporalFunctionSupport.Method(nameof(TemporalFunctions.DateDiff)),
            Expression.Constant(unit),
            buildArg(fn.Arguments[1]),
            buildArg(fn.Arguments[2]));
    }

    public Expression? BuildTyped(
        ResolvedFunctionCall fn, IReadOnlyList<ResolvedExpression> astArgs, Expression[] typedArgs) => null;
}

// ---------------------------------------------------------------------------
// Shared resolve/build helpers
// ---------------------------------------------------------------------------

internal static class TemporalFunctionSupport
{
    public static System.Reflection.MethodInfo Method(string name) =>
        typeof(TemporalFunctions).GetMethod(name)!;

    public static TemporalField ConstField(IReadOnlyList<ResolvedExpression> args, string fn)
    {
        if (args.Count >= 1 && args[0] is ResolvedLiteral { Value: Utf8String u })
        {
            return TemporalFieldNames.Parse(u.ToStringDecoded())
                ?? throw new ResolveException($"{fn}: unknown field/unit '{u.ToStringDecoded()}'");
        }

        throw new ResolveException($"{fn} requires a constant string field/unit as its first argument");
    }

    public static void RequireTemporal(string fn, ResolvedExpression a)
    {
        if (a.Type is not (SqlDateType or SqlTimeType or SqlTimestampType))
        {
            throw new ResolveException($"{fn} requires a DATE/TIME/TIMESTAMP argument, got {a.Type.Display}");
        }
    }

    public static void RequireDurationUnit(TemporalField unit, string fn)
    {
        if (unit is TemporalField.Dow or TemporalField.Doy or TemporalField.Epoch)
        {
            throw new ResolveException($"{fn} does not support the {unit} unit");
        }
    }

    public static bool IsTimeUnit(TemporalField unit) =>
        unit is TemporalField.Hour or TemporalField.Minute or TemporalField.Second;
}

// ---------------------------------------------------------------------------
// Runtime (invoked from compiled structural expression trees)
// ---------------------------------------------------------------------------

/// <summary>
/// Pure runtime helpers for the temporal scalar functions. Each takes boxed
/// <c>object?</c> operands (the structural pipeline's representation),
/// propagates NULL, and dispatches on the boxed value's temporal type.
/// </summary>
internal static class TemporalFunctions
{
    public static object? Extract(object? src, TemporalField field) => src switch
    {
        null => null,
        Date32 d => ExtractFromDate(d, field),
        Timestamp ts => ExtractFromTimestamp(ts, field),
        Time64 tm => ExtractFromTime(tm, field),
        _ => throw new InvalidOperationException($"EXTRACT: unsupported source {src.GetType().Name}"),
    };

    public static object? DateTrunc(object? src, TemporalField unit) => src switch
    {
        null => null,
        Date32 d => (object)TruncDate(d, unit),
        Timestamp ts => TruncTimestamp(ts, unit),
        Time64 tm => TruncTime(tm, unit),
        _ => throw new InvalidOperationException($"DATE_TRUNC: unsupported source {src.GetType().Name}"),
    };

    public static object? DateAdd(object? src, TemporalField unit, object? n)
    {
        if (src is null || n is null)
        {
            return null;
        }

        var iv = UnitToInterval(unit, AsLong(n));
        return src switch
        {
            Date32 d => TemporalArithmetic.AddToDate(d, iv),
            Timestamp ts => TemporalArithmetic.AddToTimestamp(ts, iv),
            Time64 tm => TemporalArithmetic.AddToTime(tm, iv),
            _ => throw new InvalidOperationException($"DATEADD: unsupported source {src.GetType().Name}"),
        };
    }

    public static object? DateDiff(TemporalField unit, object? start, object? end)
    {
        if (start is null || end is null)
        {
            return null;
        }

        return (start, end) switch
        {
            (Date32 a, Date32 b) => DiffTimestamps(ToTs(a), ToTs(b), unit),
            (Timestamp a, Timestamp b) => DiffTimestamps(a, b, unit),
            (Time64 a, Time64 b) => DiffTime(a, b, unit),
            _ => throw new InvalidOperationException("DATEDIFF: mismatched/unsupported source types"),
        };
    }

    // ---- EXTRACT ----

    // NOTE: each arm is cast to (object) explicitly. Without it the C# switch
    // expression unifies the mixed long/double arms to a single double natural
    // type, so integer fields (YEAR, MONTH, …) would box as double and mismatch
    // the resolver-assigned BIGINT column.
    private static object ExtractFromTimestamp(Timestamp ts, TemporalField field)
    {
        var dt = ts.ToDateTime();
        return field switch
        {
            TemporalField.Year => (object)(long)dt.Year,
            TemporalField.Quarter => (object)(long)((dt.Month - 1) / 3 + 1),
            TemporalField.Month => (object)(long)dt.Month,
            TemporalField.Week => (object)(long)ISOWeek.GetWeekOfYear(dt),
            TemporalField.Day => (object)(long)dt.Day,
            TemporalField.Dow => (object)(long)(int)dt.DayOfWeek,
            TemporalField.Doy => (object)(long)dt.DayOfYear,
            TemporalField.Hour => (object)(long)dt.Hour,
            TemporalField.Minute => (object)(long)dt.Minute,
            TemporalField.Second => (object)(dt.Second + SubSecond(ts.Microseconds)),
            TemporalField.Epoch => (object)(ts.Microseconds / 1_000_000.0),
            _ => throw new InvalidOperationException($"EXTRACT: bad field {field}"),
        };
    }

    private static object ExtractFromDate(Date32 d, TemporalField field)
    {
        var date = d.ToDateOnly();
        return field switch
        {
            TemporalField.Year => (object)(long)date.Year,
            TemporalField.Quarter => (object)(long)((date.Month - 1) / 3 + 1),
            TemporalField.Month => (object)(long)date.Month,
            TemporalField.Week => (object)(long)ISOWeek.GetWeekOfYear(date.ToDateTime(TimeOnly.MinValue)),
            TemporalField.Day => (object)(long)date.Day,
            TemporalField.Dow => (object)(long)(int)date.DayOfWeek,
            TemporalField.Doy => (object)(long)date.DayOfYear,
            TemporalField.Epoch => (object)((double)d.Days * 86_400.0),
            _ => throw new InvalidOperationException($"EXTRACT: bad field {field} for DATE"),
        };
    }

    private static object ExtractFromTime(Time64 tm, TemporalField field)
    {
        var micros = tm.Microseconds;
        return field switch
        {
            TemporalField.Hour => (object)(micros / Interval.MicrosPerHour),
            TemporalField.Minute => (object)(micros / Interval.MicrosPerMinute % 60),
            TemporalField.Second => (object)(micros / Interval.MicrosPerSecond % 60 + SubSecond(micros)),
            TemporalField.Epoch => (object)(micros / 1_000_000.0),
            _ => throw new InvalidOperationException($"EXTRACT: bad field {field} for TIME"),
        };
    }

    private static double SubSecond(long micros)
    {
        var m = micros % 1_000_000;
        if (m < 0)
        {
            m += 1_000_000;
        }

        return m / 1_000_000.0;
    }

    // ---- DATE_TRUNC ----

    private static Date32 TruncDate(Date32 d, TemporalField unit)
    {
        var date = d.ToDateOnly();
        var r = unit switch
        {
            TemporalField.Year => new DateOnly(date.Year, 1, 1),
            TemporalField.Quarter => new DateOnly(date.Year, ((date.Month - 1) / 3) * 3 + 1, 1),
            TemporalField.Month => new DateOnly(date.Year, date.Month, 1),
            TemporalField.Week => date.AddDays(-(((int)date.DayOfWeek + 6) % 7)),
            TemporalField.Day => date,
            _ => throw new InvalidOperationException($"DATE_TRUNC: bad unit {unit} for DATE"),
        };
        return Date32.FromDateOnly(r);
    }

    private static Timestamp TruncTimestamp(Timestamp ts, TemporalField unit)
    {
        var dt = ts.ToDateTime();
        var r = unit switch
        {
            TemporalField.Year => new DateTime(dt.Year, 1, 1),
            TemporalField.Quarter => new DateTime(dt.Year, ((dt.Month - 1) / 3) * 3 + 1, 1),
            TemporalField.Month => new DateTime(dt.Year, dt.Month, 1),
            TemporalField.Week => dt.Date.AddDays(-(((int)dt.DayOfWeek + 6) % 7)),
            TemporalField.Day => dt.Date,
            TemporalField.Hour => dt.Date.AddHours(dt.Hour),
            TemporalField.Minute => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0),
            TemporalField.Second => new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second),
            _ => throw new InvalidOperationException($"DATE_TRUNC: bad unit {unit}"),
        };
        return Timestamp.FromDateTime(r);
    }

    private static Time64 TruncTime(Time64 tm, TemporalField unit)
    {
        var micros = tm.Microseconds;
        var unitMicros = unit switch
        {
            TemporalField.Hour => Interval.MicrosPerHour,
            TemporalField.Minute => Interval.MicrosPerMinute,
            TemporalField.Second => Interval.MicrosPerSecond,
            _ => throw new InvalidOperationException($"DATE_TRUNC: bad unit {unit} for TIME"),
        };
        return new Time64(micros / unitMicros * unitMicros);
    }

    /// <summary>
    /// DATE_TRUNC in the frontier's native-unit (<see cref="long"/>) space —
    /// the monotone transform a LATENESS frontier passes through when the GC key
    /// is <c>date_trunc(unit, col)</c>. <paramref name="src"/> selects the
    /// carrier representation: TIMESTAMP/TIME = microseconds, DATE = days.
    /// </summary>
    internal static long DateTruncFrontier(long v, SqlType src, TemporalField unit) => src switch
    {
        SqlTimestampType => TruncTimestamp(new Timestamp(v), unit).Microseconds,
        SqlDateType => TruncDate(new Date32((int)v), unit).Days,
        SqlTimeType => TruncTime(new Time64(v), unit).Microseconds,
        _ => v,
    };

    // ---- TUMBLE (window assignment) ----

    /// <summary>Floor a temporal value to the start of its fixed-size window — the
    /// runtime of <c>tumble_start</c>. <paramref name="size"/> is in the source's
    /// native unit (microseconds for TIMESTAMP, whole days for DATE), matching
    /// <see cref="FloorToBucket"/> so the structural result and the GC frontier
    /// transform agree exactly.</summary>
    public static object? TumbleStart(object? src, long size) => src switch
    {
        null => null,
        Timestamp ts => (object)new Timestamp(FloorToBucket(ts.Microseconds, size)),
        Date32 d => (object)new Date32((int)FloorToBucket(d.Days, size)),
        _ => throw new InvalidOperationException($"tumble_start: unsupported source {src.GetType().Name}"),
    };

    /// <summary>Floor <paramref name="v"/> to the largest multiple of
    /// <paramref name="size"/> not exceeding it (correct for negative
    /// <paramref name="v"/> — epoch-relative times can precede 1970). Doubles as the
    /// monotone frontier transform for a window-start key.</summary>
    public static long FloorToBucket(long v, long size) => FloorDiv(v, size) * size;

    // ---- DATEADD ----

    private static Interval UnitToInterval(TemporalField unit, long k) => unit switch
    {
        TemporalField.Year => new Interval(checked((int)(k * 12)), 0),
        TemporalField.Quarter => new Interval(checked((int)(k * 3)), 0),
        TemporalField.Month => new Interval(checked((int)k), 0),
        TemporalField.Week => new Interval(0, k * 7 * Interval.MicrosPerDay),
        TemporalField.Day => new Interval(0, k * Interval.MicrosPerDay),
        TemporalField.Hour => new Interval(0, k * Interval.MicrosPerHour),
        TemporalField.Minute => new Interval(0, k * Interval.MicrosPerMinute),
        TemporalField.Second => new Interval(0, k * Interval.MicrosPerSecond),
        _ => throw new InvalidOperationException($"DATEADD: bad unit {unit}"),
    };

    // ---- DATEDIFF ----

    private static Timestamp ToTs(Date32 d) => new(d.Days * Interval.MicrosPerDay);

    private static long DiffTimestamps(Timestamp s, Timestamp e, TemporalField unit)
    {
        var sd = s.ToDateTime();
        var ed = e.ToDateTime();
        return unit switch
        {
            TemporalField.Year => ed.Year - sd.Year,
            TemporalField.Quarter => (ed.Year * 4L + (ed.Month - 1) / 3) - (sd.Year * 4L + (sd.Month - 1) / 3),
            TemporalField.Month => (ed.Year * 12L + ed.Month) - (sd.Year * 12L + sd.Month),
            TemporalField.Week => FloorDiv(DayNumber(ed), 7) - FloorDiv(DayNumber(sd), 7),
            TemporalField.Day => DayNumber(ed) - DayNumber(sd),
            TemporalField.Hour => FloorDiv(e.Microseconds, Interval.MicrosPerHour) - FloorDiv(s.Microseconds, Interval.MicrosPerHour),
            TemporalField.Minute => FloorDiv(e.Microseconds, Interval.MicrosPerMinute) - FloorDiv(s.Microseconds, Interval.MicrosPerMinute),
            TemporalField.Second => FloorDiv(e.Microseconds, Interval.MicrosPerSecond) - FloorDiv(s.Microseconds, Interval.MicrosPerSecond),
            _ => throw new InvalidOperationException($"DATEDIFF: bad unit {unit}"),
        };
    }

    private static long DiffTime(Time64 s, Time64 e, TemporalField unit)
    {
        var unitMicros = unit switch
        {
            TemporalField.Hour => Interval.MicrosPerHour,
            TemporalField.Minute => Interval.MicrosPerMinute,
            TemporalField.Second => Interval.MicrosPerSecond,
            _ => throw new InvalidOperationException($"DATEDIFF: unit {unit} not valid for TIME"),
        };
        return FloorDiv(e.Microseconds, unitMicros) - FloorDiv(s.Microseconds, unitMicros);
    }

    private static long DayNumber(DateTime dt) => DateOnly.FromDateTime(dt).DayNumber;

    private static long FloorDiv(long a, long b)
    {
        var q = a / b;
        if (a % b != 0 && (a < 0) != (b < 0))
        {
            q--;
        }

        return q;
    }

    private static long AsLong(object o) => o switch
    {
        int i => i,
        long l => l,
        _ => Convert.ToInt64(o, CultureInfo.InvariantCulture),
    };
}
