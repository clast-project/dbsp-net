// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Connectors.Abstractions;

/// <summary>
/// Where a <see cref="ProgramRunner"/> takes the logical clock that <c>NOW()</c> reads
/// (docs/design-program-temporal.md §2 Q1). Before every step of a program that reads the clock,
/// the runner samples <see cref="Time"/>, rounds it down to <see cref="ResolutionMicros"/>, and
/// advances the clock to the larger of that and its current value: a wall clock that steps back
/// holds the clock, as Feldera's <c>MAX(now)</c> does. Tests pass a <see cref="TimeProvider"/> they
/// control.
/// </summary>
/// <param name="Time">The time source; <see cref="TimeProvider.System"/> by default.</param>
/// <param name="ResolutionMicros">The clock's granularity in microseconds; 1 s by default, as in
/// Feldera (<c>clock_resolution_usecs</c>).</param>
public sealed record ProgramClock(TimeProvider Time, long ResolutionMicros = ProgramClock.DefaultResolutionMicros)
{
    /// <summary>Feldera's default clock resolution, 1 s.</summary>
    public const long DefaultResolutionMicros = 1_000_000;

    /// <summary>The wall clock at a 1 s resolution.</summary>
    public static ProgramClock Default { get; } = new(TimeProvider.System);

    public TimeProvider Time { get; } = Time ?? throw new ArgumentNullException(nameof(Time));

    public long ResolutionMicros { get; } = ResolutionMicros >= 1
        ? ResolutionMicros
        : throw new ArgumentOutOfRangeException(nameof(ResolutionMicros), ResolutionMicros, "the clock resolution must be at least 1 µs");

    /// <summary>The current time in microseconds since the epoch, rounded down to the resolution.</summary>
    public long Sample()
    {
        var micros = (Time.GetUtcNow() - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;
        return micros - (((micros % ResolutionMicros) + ResolutionMicros) % ResolutionMicros);
    }
}
