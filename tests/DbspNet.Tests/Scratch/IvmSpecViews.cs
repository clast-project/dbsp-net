// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Tests.Scratch;

/// <summary>
/// The one rule every ivm-bench probe has to get right: <b>which views the program must compute</b>.
/// </summary>
/// <remarks>
/// <para>The deploy spec carries two lists. <c>output_bindings</c> (16) are the views written to a
/// Delta sink. <c>outputs</c> (18) are the views dbt declares — including two,
/// <c>fact_market_history</c> and <c>daily_market_pulse</c>, marked <c>+stored: true</c> with the
/// output connector deliberately removed because at SF=100 a truncate-mode writer for them never
/// drains. Feldera computes those two and the benchmark measures that compute; only the Delta write
/// is skipped.</para>
/// <para>Every probe here used to build its compile set from <c>output_bindings</c>. A view that is
/// not an output is not reachable, so <c>CompileProgram</c>'s dead-view prune dropped both — and
/// with them the whole <c>finwire_financial → financials → wrk_company_financials →
/// fact_market_history</c> chain, plus the <c>daily_market</c> window operators that exist only to
/// feed it. That is work Feldera does and we did not, in every batch-1 number we have quoted
/// (<c>docs/comparison-feldera-decisions.md</c> §6.3).</para>
/// <para>Use <see cref="ToCompile"/> for the compile set and the bindings only for wiring sinks.
/// The fallback keeps an older spec (no <c>outputs</c> key) working, which is also what makes the
/// asymmetry measurable: run a probe against both and the difference is what the pruned chain
/// costs.</para>
/// </remarks>
internal static class IvmSpecViews
{
    public static HashSet<string> ToCompile(
        IEnumerable<string>? specOutputs, IEnumerable<string> boundViews)
    {
        var set = new HashSet<string>(boundViews, StringComparer.Ordinal);
        if (specOutputs is not null && Environment.GetEnvironmentVariable("IVM_BINDINGS_ONLY") is not ("1" or "true" or "TRUE"))
        {
            set.UnionWith(specOutputs);
        }

        return set;
    }
}
