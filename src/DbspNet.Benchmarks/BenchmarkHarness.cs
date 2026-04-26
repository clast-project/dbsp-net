using System.Diagnostics;

namespace DbspNet.Benchmarks;

/// <summary>
/// Timing helpers for the perf benchmarks. Deliberately small — no
/// BenchmarkDotNet dependency, just <see cref="Stopwatch"/> + sorted runs.
/// Resolution: sub-microsecond on modern x64 (<see cref="Stopwatch.IsHighResolution"/>
/// is expected to be true). For sub-µs operations we take the median of
/// many per-iteration measurements; for ms-scale operations we take the
/// median of a handful of full-run measurements.
/// </summary>
internal static class BenchmarkHarness
{
    /// <summary>
    /// Measure a "cold" operation: the whole of <paramref name="run"/> is
    /// timed (including anything <paramref name="setup"/> put in place,
    /// since setup runs <em>before</em> the Stopwatch starts).
    /// </summary>
    public static double MedianColdMs<TState>(
        Func<TState> setup,
        Action<TState> run,
        int warmups = 1,
        int runs = 5)
    {
        var times = new List<double>();
        for (var i = 0; i < warmups + runs; i++)
        {
            var state = setup();
            var sw = Stopwatch.StartNew();
            run(state);
            sw.Stop();
            if (i >= warmups)
            {
                times.Add(sw.Elapsed.TotalMilliseconds);
            }
        }

        times.Sort();
        return times[times.Count / 2];
    }

    /// <summary>
    /// Measure per-step latency in a "warm" circuit: setup produces a
    /// loaded / warmed <typeparamref name="TState"/>; we then run
    /// <paramref name="oneStep"/> repeatedly, timing each individual call.
    /// Returns the median in microseconds.
    /// </summary>
    public static double MedianPerStepMicros<TState>(
        Func<TState> setup,
        Action<TState> oneStep,
        int warmupSteps = 20,
        int measureSteps = 100)
    {
        var state = setup();
        for (var i = 0; i < warmupSteps; i++)
        {
            oneStep(state);
        }

        var times = new List<double>(measureSteps);
        var sw = new Stopwatch();
        for (var i = 0; i < measureSteps; i++)
        {
            sw.Restart();
            oneStep(state);
            sw.Stop();
            times.Add(sw.Elapsed.TotalMilliseconds * 1000.0);
        }

        times.Sort();
        return times[times.Count / 2];
    }

    public static string FormatMs(double ms) =>
        ms switch
        {
            < 1.0 => $"{ms * 1000.0,7:F1} µs",
            < 1000.0 => $"{ms,7:F2} ms",
            _ => $"{ms / 1000.0,7:F2} s",
        };

    public static string FormatMicros(double us) =>
        us switch
        {
            < 1.0 => $"{us * 1000.0,7:F1} ns",
            < 1000.0 => $"{us,7:F2} µs",
            _ => $"{us / 1000.0,7:F2} ms",
        };

    public static string FormatRatio(double ratio) =>
        ratio switch
        {
            >= 100.0 => $"{ratio,6:F0}×",
            >= 10.0 => $"{ratio,6:F1}×",
            _ => $"{ratio,6:F2}×",
        };
}
