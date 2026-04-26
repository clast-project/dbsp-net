using DbspNet.Core.Algebra;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;

namespace DbspNet.Core.Operators.Stateful;

/// <summary>
/// Incremental <c>distinct</c>: receives a stream of Z-set deltas and emits a
/// stream of Z-set deltas such that the cumulative output for every key is
/// either <c>One</c> (if the cumulative input weight is strictly positive)
/// or <c>Zero</c>. A row is emitted with weight +1 the first tick it becomes
/// present, and -1 the tick it becomes absent — exactly mirroring SQL
/// <c>DISTINCT</c> semantics under retractions.
/// </summary>
internal sealed class DistinctOp<TKey, TWeight> : IOperator
    where TKey : notnull
    where TWeight : struct, IZRing<TWeight>
{
    private readonly Stream<ZSet<TKey, TWeight>> _input;
    private readonly Stream<ZSet<TKey, TWeight>> _output;
    private readonly ZSetTrace<TKey, TWeight> _trace = new();

    public DistinctOp(Stream<ZSet<TKey, TWeight>> input, Stream<ZSet<TKey, TWeight>> output)
    {
        _input = input;
        _output = output;
    }

    public void Step()
    {
        var delta = _input.Current;
        if (delta.IsEmpty)
        {
            _output.SetCurrent(ZSet<TKey, TWeight>.Empty);
            return;
        }

        var outputBuilder = new ZSetBuilder<TKey, TWeight>();
        foreach (var (key, dw) in delta)
        {
            var before = _trace.Current.WeightOf(key);
            var after = TWeight.Add(before, dw);

            var wasPresent = TWeight.IsPositive(before);
            var isPresent = TWeight.IsPositive(after);

            if (!wasPresent && isPresent)
            {
                outputBuilder.Add(key, TWeight.One);
            }
            else if (wasPresent && !isPresent)
            {
                outputBuilder.Add(key, TWeight.Negate(TWeight.One));
            }
        }

        _output.SetCurrent(outputBuilder.Build());
        _trace.Integrate(delta);
    }
}
