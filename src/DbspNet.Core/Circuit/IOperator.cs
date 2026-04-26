namespace DbspNet.Core.Circuit;

/// <summary>
/// Represents a single node in a DBSP circuit. Operators are scheduled in
/// topological order and fire exactly once per tick.
/// </summary>
internal interface IOperator
{
    void Step();
}
