// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// Shared bound for the ParallelCircuit tests that drive a barrier which could, if the code under
// test is broken, never release.
//
// These tests were written as `Assert.ThrowsAsync<AggregateException>(() => Task.Run(pc.Step)
// .WaitAsync(TimeSpan.FromSeconds(10)))`, which conflates two different failures. The bound is a
// HANG GUARD — "a throwing worker stranded the barrier and Step will never return" — but folding it
// inside the exception assertion means a merely SLOW run reports as
//
//     Assert.Throws() Failure: Exception type was not an exact match
//     Expected: typeof(System.AggregateException)
//     Actual:   typeof(System.TimeoutException)
//
// which is indistinguishable from the real defect. That was observed intermittently on a loaded
// machine (one failure in five consecutive full-suite runs).
//
// So: check the bound separately from the assertion, and make it generous. A longer bound does not
// weaken the guard — a genuine hang still fails the test, just later — while a short one turns
// machine load into false failures.
using Xunit;

namespace DbspNet.Tests.Circuit;

internal static class HangGuard
{
    /// <summary>
    /// How long a circuit step may take before we conclude it will never return. Deliberately far
    /// above any plausible real duration: this is a liveness bound, not a performance assertion.
    /// </summary>
    internal static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Runs <paramref name="action"/> and asserts it threw <typeparamref name="T"/>. A run that
    /// exceeds <see cref="Timeout"/> fails as a stranded barrier, not as a wrong exception type.
    /// </summary>
    internal static async Task<T> ThrowsWithoutHangingAsync<T>(string what, Action action)
        where T : Exception
    {
        var task = await RunBoundedAsync(what, action);
        return await Assert.ThrowsAsync<T>(() => task);
    }

    /// <summary>
    /// Runs <paramref name="action"/> and asserts it completed. Any exception it threw propagates
    /// unchanged; only a hang is reported specially.
    /// </summary>
    internal static async Task CompletesWithoutHangingAsync(string what, Action action)
    {
        var task = await RunBoundedAsync(what, action);
        await task;
    }

    /// <summary>Returns the completed task, or fails with a hang diagnosis.</summary>
    private static async Task<Task> RunBoundedAsync(string what, Action action)
    {
        var task = Task.Run(action);

        // Cancel the timer once the work wins the race, so a 60s delay is not left pending for
        // every call in the suite.
        using var cts = new CancellationTokenSource();
        var finished = await Task.WhenAny(task, Task.Delay(Timeout, cts.Token)).ConfigureAwait(false);
        await cts.CancelAsync().ConfigureAwait(false);

        Assert.True(
            ReferenceEquals(finished, task),
            $"{what} did not return within {Timeout.TotalSeconds:F0}s — a worker exception most " +
            "likely stranded the barrier. This is the liveness guard, not a timing assertion, so " +
            "machine load should not reach it.");

        return task;
    }
}
