// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using CsCheck;
using DbspNet.Core.Algebra;
using DbspNet.Core.Collections;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.EndToEnd;

/// <summary>
/// Program-path analogue of <see cref="LatenessPbtTests"/> (issue #3): a two-view program
/// (<c>mid</c> over the LATENESS tables, <c>out</c> over <c>mid</c>) compiled with input
/// late-drop and cross-view frontier GC must agree, on every output view, with the batch
/// evaluation over the admitted rows. <c>mid</c> is either an output (its own stream, scanned by
/// <c>out</c> with seeded verdicts) or single-use (inlined into <c>out</c>), so both
/// cross-view routes are exercised. GC is invisible by construction, so any divergence is a
/// cross-view frontier that collected live state.
/// </summary>
public class ProgramLatenessPbtTests
{
    private static readonly Gen<int> GenLateness = Gen.Int[0, 12];

    // (mid, out): every out is a GC site on a column whose monotonicity comes from mid.
    private static readonly Gen<(string Mid, string Out)> GenShape = Gen.OneOfConst(
        ("SELECT ts, v FROM a WHERE v > 0",
            "SELECT ts, COUNT(*) AS c FROM mid GROUP BY ts"),
        ("SELECT ts + 3 AS shifted, v FROM a",
            "SELECT shifted, SUM(v) AS s FROM mid GROUP BY shifted"),
        ("SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
            "SELECT m.ts, m.s, b.v FROM mid m JOIN b ON m.ts = b.ts"),
        ("SELECT ts, SUM(v) AS s FROM a GROUP BY ts",
            "SELECT m.ts, m.s, b.v FROM mid m LEFT JOIN b ON m.ts = b.ts"),
        ("SELECT ts FROM a UNION SELECT ts FROM b",
            "SELECT ts, COUNT(*) AS c FROM mid GROUP BY ts"),
        ("SELECT a.ts, a.v FROM a JOIN b ON a.ts = b.ts",
            "SELECT ts, SUM(v) AS s FROM mid GROUP BY ts"),
        ("SELECT ts, COUNT(*) AS c FROM a GROUP BY ts",
            "SELECT DISTINCT ts FROM mid"));

    private static readonly Gen<InputEvent> GenEvent = Gen.OneOf(
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5]).Select(p => new InputEvent("a", [(long)p.Item1, p.Item2], 1L)),
        Gen.Select(Gen.Int[0, 30], Gen.Int[-2, 5]).Select(p => new InputEvent("b", [(long)p.Item1, p.Item2], 1L)));

    private static readonly Gen<IReadOnlyList<IReadOnlyList<InputEvent>>> GenTicks =
        GenEvent.Array[0, 5]
            .Select(arr => (IReadOnlyList<InputEvent>)arr)
            .Array[1, 8]
            .Select(arr => (IReadOnlyList<IReadOnlyList<InputEvent>>)arr);

    [Fact]
    public void ProgramLatenessIncrementalEqualsBatchOverAdmitted_Flat()
    {
        Gen.Select(GenLateness, GenShape, Gen.Bool, GenTicks)
            .Sample((d, shape, midIsOutput, ticks) => CheckOne(d, shape, midIsOutput, ticks, options: null), iter: 1000);
    }

    [Fact]
    public void ProgramLatenessIncrementalEqualsBatchOverAdmitted_Spine()
    {
        Gen.Select(GenLateness, GenShape, Gen.Bool, GenTicks)
            .Sample(
                (d, shape, midIsOutput, ticks) => CheckOne(
                    d, shape, midIsOutput, ticks, new CompileOptions { TraceFamily = TraceFamily.Spine }),
                iter: 1000);
    }

    // Parallel arms (docs/design-parallel-frontier.md): the W-replica program must equal the SERIAL
    // program on every output view after every tick, at W = 2, 3 and 4.
    [Fact]
    public void ProgramLatenessParallelEqualsSerial_Flat()
    {
        Gen.Select(GenLateness, GenShape, Gen.Bool, GenTicks)
            .Sample((d, shape, midIsOutput, ticks) => CheckParallel(d, shape, midIsOutput, ticks, options: null), iter: 200);
    }

    [Fact]
    public void ProgramLatenessParallelEqualsSerial_Spine()
    {
        Gen.Select(GenLateness, GenShape, Gen.Bool, GenTicks)
            .Sample(
                (d, shape, midIsOutput, ticks) => CheckParallel(
                    d, shape, midIsOutput, ticks, new CompileOptions { TraceFamily = TraceFamily.Spine }),
                iter: 200);
    }

    private static string[] Program(int lateness, (string Mid, string Out) shape) =>
    [
        $"CREATE TABLE a (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
        $"CREATE TABLE b (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
        $"CREATE VIEW mid AS {shape.Mid}",
        $"CREATE VIEW out AS {shape.Out}",
    ];

    private static bool CheckParallel(
        int lateness,
        (string Mid, string Out) shape,
        bool midIsOutput,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks,
        CompileOptions? options)
    {
        var sql = Program(lateness, shape);
        var outputs = midIsOutput
            ? new HashSet<string>(["mid", "out"], StringComparer.Ordinal)
            : new HashSet<string>(["out"], StringComparer.Ordinal);
        var serial = SqlProgram.Compile(sql, outputs, options: options);
        var parallels = new List<ParallelCompiledProgram>();
        try
        {
            foreach (var w in new[] { 2, 3, 4 })
            {
                Assert.True(
                    SqlProgram.TryCompileParallel(sql, outputs, w, out var p, options: options),
                    $"parallel compile refused: {shape}");
                parallels.Add(p!);
            }

            for (var t = 0; t < ticks.Count; t++)
            {
                foreach (var ev in ticks[t])
                {
                    serial.Table(ev.Table).Insert(ev.Row);
                    foreach (var p in parallels)
                    {
                        p.Table(ev.Table).Insert(ev.Row);
                    }
                }

                serial.Step();
                foreach (var p in parallels)
                {
                    p.Step();
                    foreach (var name in outputs)
                    {
                        var expected = serial.Outputs[name].CurrentView;
                        var actual = p.Outputs[name].CurrentView;
                        if (!actual.Equals(expected))
                        {
                            Console.Error.WriteLine(
                                $"mid: {shape.Mid}  out: {shape.Out}  LATENESS {lateness}  midIsOutput={midIsOutput}  " +
                                $"W={p.Workers}  tick {t}  trace={options?.TraceFamily ?? TraceFamily.Flat}  view={name}");
                            Console.Error.WriteLine("serial:   " + expected);
                            Console.Error.WriteLine("parallel: " + actual);
                            return false;
                        }
                    }
                }
            }

            return true;
        }
        finally
        {
            foreach (var p in parallels)
            {
                p.Dispose();
            }
        }
    }

    private static bool CheckOne(
        int lateness,
        (string Mid, string Out) shape,
        bool midIsOutput,
        IReadOnlyList<IReadOnlyList<InputEvent>> ticks,
        CompileOptions? options)
    {
        string[] sql =
        [
            $"CREATE TABLE a (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
            $"CREATE TABLE b (ts BIGINT NOT NULL LATENESS {lateness}, v INT NOT NULL)",
            $"CREATE VIEW mid AS {shape.Mid}",
            $"CREATE VIEW out AS {shape.Out}",
        ];
        var outputs = midIsOutput
            ? new HashSet<string>(["mid", "out"], StringComparer.Ordinal)
            : new HashSet<string>(["out"], StringComparer.Ordinal);
        var program = SqlProgram.Compile(sql, outputs, options: options);

        foreach (var tick in ticks)
        {
            foreach (var ev in tick)
            {
                program.Table(ev.Table).Insert(ev.Row);
            }

            program.Step();
        }

        // Oracle: the same start-of-tick admit rule, then batch-evaluate mid and out in order
        // (out's scan of mid reads mid's batch answer).
        var admitted = LatenessPbtTests.SimulateAdmit(ticks, lateness);
        var states = new Dictionary<string, ZSet<StructuralRow, Z64>>(StringComparer.Ordinal)
        {
            ["a"] = IncrementalOracle.NetTable(admitted, "a"),
            ["b"] = IncrementalOracle.NetTable(admitted, "b"),
        };
        var resolved = SqlProgram.Resolve(sql, outputs);
        foreach (var view in resolved.Views)
        {
            var ctx = new BatchEvalContext(states, new Dictionary<CteRef, ZSet<StructuralRow, Z64>>());
            states[view.ViewName] = BatchPlanEvaluator.Evaluate(view.Query, ctx);
        }

        foreach (var name in outputs)
        {
            var actual = program.Outputs[name].CurrentView;
            if (!actual.Equals(states[name]))
            {
                Console.Error.WriteLine(
                    $"mid: {shape.Mid}  out: {shape.Out}  LATENESS {lateness}  midIsOutput={midIsOutput}  " +
                    $"trace={options?.TraceFamily ?? TraceFamily.Flat}  view={name}");
                Console.Error.WriteLine("admitted: [" + string.Join(", ", admitted.Select(e => e.ToString())) + "]");
                Console.Error.WriteLine("program: " + actual);
                Console.Error.WriteLine("batch:   " + states[name]);
                return false;
            }
        }

        return true;
    }
}
