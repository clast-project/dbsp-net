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
        Assert.Null(p.Out.Current); // released once r had read it (the plan applies from tick 2)
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
}
