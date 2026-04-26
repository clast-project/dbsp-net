namespace DbspNet.Core.Circuit.Operators;

/// <summary>
/// The z^-1 delay operator. On tick <c>t</c> it emits the value that the
/// input carried on tick <c>t-1</c>; on tick 0 it emits the supplied initial
/// value.
/// </summary>
/// <remarks>
/// This operator assumes it runs in topological order AFTER its input
/// producer. Feedback cycles (where the input depends on the delay's output
/// transitively) require iterative scheduling and are not supported in v1 —
/// see <c>docs/skipped.md</c>.
/// </remarks>
internal sealed class DelayOp<T> : IOperator
{
    private readonly Stream<T> _input;
    private readonly Stream<T> _output;
    private T _nextOutput;

    public DelayOp(Stream<T> input, Stream<T> output, T initial)
    {
        _input = input;
        _output = output;
        _nextOutput = initial;
    }

    public void Step()
    {
        // Publish what we captured last tick (initial value on tick 0),
        // then capture this tick's input for next time.
        _output.SetCurrent(_nextOutput);
        _nextOutput = _input.Current;
    }
}
