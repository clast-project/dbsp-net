// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Persistence.Runs;

/// <summary>
/// Which on-disk layout the trace codecs write (docs/design-durable-state.md §1f, increment 2):
/// today's single Arrow stream per file, or a <see cref="RunFile"/> (hash-ordered blocks with an
/// index and a Bloom filter). <c>DBSPNET_STATE_LAYOUT=runs</c> selects runs; default off. Reading
/// does not depend on it — a codec recognises either format by the file's trailer.
/// </summary>
public static class StateLayout
{
    private static readonly bool FromEnvironment =
        string.Equals(Environment.GetEnvironmentVariable("DBSPNET_STATE_LAYOUT"), "runs", StringComparison.OrdinalIgnoreCase);

    // Flows with the async context (into Task.Run'd per-worker writes), so a test can switch it
    // for its own snapshots without touching any other test's.
    private static readonly AsyncLocal<bool?> Override = new();

    /// <summary>Whether the trace codecs write runs.</summary>
    public static bool Runs => Override.Value ?? FromEnvironment;

    /// <summary>Write runs (or not) in the current async context until the result is disposed.</summary>
    public static IDisposable Use(bool runs)
    {
        var previous = Override.Value;
        Override.Value = runs;
        return new Scope(previous);
    }

    private sealed class Scope(bool? previous) : IDisposable
    {
        public void Dispose() => Override.Value = previous;
    }
}
