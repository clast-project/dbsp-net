// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Core.Circuit;
using Xunit;

namespace DbspNet.Tests.Circuit;

/// <summary>Releasing a tick's stream values after their last reader (docs/design-bounded-memory.md §3b).</summary>
public class StreamReleaseTests
{
    private sealed class Producer : IOperator
    {
        public readonly Stream<object> Out = new();
        private int _tick;

        public void Step() => Out.SetCurrent(++_tick);
    }

    // Reads its input through a field the scan walks.
    private sealed class FieldReader(Stream<object> input) : IOperator
    {
        public readonly List<object?> Seen = [];

        public void Step() => Seen.Add(input.Current);
    }

    // Reads its input through a dictionary.
    private sealed class DictionaryReader(Stream<object> input) : IOperator
    {
        private readonly Dictionary<string, Stream<object>> _inputs = new() { ["in"] = input };
        public readonly List<object?> Seen = [];

        public void Step() => Seen.Add(_inputs["in"].Current);
    }

    // Reads its input through a lazy sequence: found through the iterator's fields.
    private sealed class LazyReader(Stream<object> input) : IOperator
    {
        private readonly IEnumerable<Stream<object>> _inputs = Yield(input);
        public readonly List<object?> Seen = [];

        public void Step() => Seen.Add(_inputs.First().Current);

        private static IEnumerable<Stream<object>> Yield(Stream<object> s)
        {
            yield return s;
        }
    }

    // Reads its input from the end of a collection too large to walk.
    private sealed class HugeListReader(Stream<object> input) : IOperator
    {
        private readonly List<object> _inputs = [.. Enumerable.Repeat<object>("filler", 70_000), input];
        public readonly List<object?> Seen = [];

        public void Step() => Seen.Add(((Stream<object>)_inputs[^1]).Current);
    }

    private static RootCircuit Build(params IOperator[] ops) =>
        RootCircuit.Build(builder =>
        {
            foreach (var op in ops)
            {
                builder.AddRawOperator(op);
            }
        });

    [Fact]
    public void A_value_is_released_after_its_last_reader()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        var p = new Producer();
        var r = new FieldReader(p.Out);
        var circuit = Build(p, r);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        Assert.Equal([1, 2, 3], r.Seen.Cast<int>());
        Assert.Null(p.Out.Current); // released once r had read it
    }

    [Fact]
    public void A_reader_through_a_collection_is_found()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        var p = new Producer();
        var r = new FieldReader(p.Out);
        var d = new DictionaryReader(p.Out);
        var circuit = Build(p, r, d);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        Assert.Equal([1, 2, 3], d.Seen.Cast<int>());
        Assert.Null(p.Out.Current); // released, after d
    }

    [Fact]
    public void A_reader_through_a_lazy_sequence_is_found()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        var p = new Producer();
        var r = new FieldReader(p.Out);
        var lazy = new LazyReader(p.Out);
        var circuit = Build(p, r, lazy);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        Assert.Equal([1, 2, 3], lazy.Seen.Cast<int>());
        Assert.Null(p.Out.Current);
    }

    [Fact]
    public void An_operator_the_scan_cannot_see_into_keeps_every_later_value_alive()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        var p = new Producer();
        var r = new FieldReader(p.Out);
        var huge = new HugeListReader(p.Out);
        var circuit = Build(p, r, huge);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        // Without the opaque rule the value is released after r, and the huge list's reader reads null.
        Assert.Equal([1, 2, 3], huge.Seen.Cast<int>());
        Assert.Null(p.Out.Current); // still released, after it
    }

    // Emits a list of tick-many items: a value with a Count.
    private sealed class ListProducer : IOperator
    {
        public readonly Stream<List<int>> Out = new();
        private int _tick;

        public void Step() => Out.SetCurrent([.. Enumerable.Range(0, ++_tick)]);
    }

    private sealed class ListReader(Stream<List<int>> input) : IOperator
    {
        public readonly List<int> Counts = [];

        public void Step() => Counts.Add(input.Current.Count);
    }

    [Fact]
    public void The_first_tick_releases_too()
    {
        if (!StreamRelease.Enabled || !StreamRelease.FirstTickEnabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0 or DBSPNET_STREAM_RELEASE_FIRST_TICK=0
        }

        // §5.6 (R0): a batch that is one tick held every value to its end.
        var p = new Producer();
        var r = new FieldReader(p.Out);
        var circuit = Build(p, r);
        circuit.Step();

        Assert.Equal([1], r.Seen.Cast<int>());
        Assert.Null(p.Out.Current);
    }

    [Fact]
    public void A_reader_before_its_producer_keeps_the_value_in_every_tick()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        // Feedback: before reads the previous tick's value, so releasing after the reader that
        // follows the producer would hand it null next tick. The value is never released — in the
        // first tick either, where the producer is only known once it writes.
        var p = new Producer();
        var before = new FieldReader(p.Out);
        var after = new FieldReader(p.Out);
        var circuit = Build(before, p, after);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        Assert.Equal([null, 1, 2], before.Seen);
        Assert.Equal([1, 2, 3], after.Seen.Cast<int>());
        Assert.Equal(3, p.Out.Current);
    }

    [Fact]
    public void A_released_value_keeps_its_count()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        // Introspection (LastOutputRows) reads the count after the step, when release has dropped
        // the value: it read 0 from the second tick on (and, with R0, from the first).
        var p = new ListProducer();
        var r = new ListReader(p.Out);
        var circuit = Build(p, r);
        for (var i = 1; i <= 3; i++)
        {
            circuit.Step();
            Assert.Null(p.Out.Current);
            Assert.Equal(i, p.Out.LastCount);
        }

        Assert.Equal([1, 2, 3], r.Counts);
    }

    [Fact]
    public void An_input_is_released_after_its_last_reader_and_reads_zero_on_a_quiet_tick()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        // §5.10: an input is written by its handle's commit before every step (its zero when nothing
        // was pushed), so it can go once its last reader has run — in the first tick too, where a
        // bulk load's whole input otherwise lived to the next tick.
        InputHandle<object> handle = null!;
        Stream<object> input = null!;
        FieldReader first = null!, last = null!;
        var circuit = RootCircuit.Build(b =>
        {
            (handle, input) = b.Input<object>(0, (_, y) => y);
            first = new FieldReader(input);
            last = new FieldReader(input);
            b.AddRawOperator(first);
            b.AddRawOperator(last);
        });

        handle.Push(1);
        circuit.Step();
        Assert.Null(input.Current);
        circuit.Step(); // nothing pushed: the readers see the zero, not a released null
        Assert.Null(input.Current);
        handle.Push(3);
        circuit.Step();
        Assert.Null(input.Current);

        Assert.Equal([1, 0, 3], first.Seen.Cast<int>());
        Assert.Equal([1, 0, 3], last.Seen.Cast<int>());
    }

    [Fact]
    public void A_stream_written_outside_the_step_but_not_every_tick_is_kept()
    {
        if (!StreamRelease.Enabled)
        {
            return; // DBSPNET_STREAM_RELEASE=0
        }

        // Written once, before any step, and read every tick (a value set from outside, like a
        // clock advanced only now and then): unlike an input it is not written again, so releasing
        // it would hand later ticks null.
        var external = new Stream<object>();
        external.SetCurrent(5);
        var reader = new FieldReader(external);
        var circuit = Build(reader);
        for (var i = 0; i < 3; i++)
        {
            circuit.Step();
        }

        Assert.Equal([5, 5, 5], reader.Seen.Cast<int>());
    }
}
