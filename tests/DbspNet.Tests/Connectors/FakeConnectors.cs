// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using SqlSchema = DbspNet.Sql.Plan.Schema;
using ArrowSchema = Apache.Arrow.Schema;

namespace DbspNet.Tests.Connectors;

/// <summary>
/// Build/decode Arrow <see cref="RecordBatch"/>es from plain rows for the connector
/// tests, covering the types the tests use (Int32 / Int64 / String, with nulls). The
/// schema is a DbspNet <see cref="SqlSchema"/> mapped to Arrow via
/// <see cref="ArrowSchemaBridge"/>.
/// </summary>
internal static class ArrowTestData
{
    public static RecordBatch Batch(SqlSchema schema, IReadOnlyList<object?[]> rows)
    {
        var arrowSchema = ArrowSchemaBridge.ToArrow(schema);
        var arrays = new IArrowArray[schema.Count];
        for (var c = 0; c < schema.Count; c++)
        {
            arrays[c] = BuildColumn(arrowSchema.FieldsList[c].DataType, rows, c);
        }

        return new RecordBatch(arrowSchema, arrays, rows.Count);
    }

    private static IArrowArray BuildColumn(IArrowType type, IReadOnlyList<object?[]> rows, int c)
    {
        switch (type)
        {
            case Int32Type:
                {
                    var b = new Int32Array.Builder();
                    foreach (var r in rows)
                    {
                        if (r[c] is null) b.AppendNull(); else b.Append(Convert.ToInt32(r[c], System.Globalization.CultureInfo.InvariantCulture));
                    }

                    return b.Build();
                }

            case Int64Type:
                {
                    var b = new Int64Array.Builder();
                    foreach (var r in rows)
                    {
                        if (r[c] is null) b.AppendNull(); else b.Append(Convert.ToInt64(r[c], System.Globalization.CultureInfo.InvariantCulture));
                    }

                    return b.Build();
                }

            case StringType:
                {
                    var b = new StringArray.Builder();
                    foreach (var r in rows)
                    {
                        if (r[c] is null) b.AppendNull(); else b.Append((string)r[c]!);
                    }

                    return b.Build();
                }

            default:
                throw new NotSupportedException($"ArrowTestData does not build {type.Name} columns");
        }
    }
}

/// <summary>
/// A scripted, replayable input source. Constructed with a schema and a list of
/// versions (each a set of (row, weight) changes). <see cref="NextAsync"/> returns one
/// version at a time — the "one tick per version" contract — and is replayable: it
/// serves version <c>from+1</c> regardless of prior calls.
/// </summary>
internal sealed class FakeInputConnector : IInputConnector
{
    private readonly SqlSchema _schema;
    private readonly IReadOnlyList<IReadOnlyList<(object?[] Row, long Weight)>> _versions;

    public FakeInputConnector(
        string name,
        SqlSchema schema,
        IReadOnlyList<IReadOnlyList<(object?[] Row, long Weight)>> versions)
    {
        Name = name;
        _schema = schema;
        _versions = versions;
    }

    public string Name { get; }

    public IConnectorOffset InitialOffset => LongOffset.Before;

    public IConnectorOffset ParseOffset(string serialized) => LongOffset.Parse(serialized);

    public ValueTask<SqlSchema> ResolveSchemaAsync(SqlSchema? declared, CancellationToken cancellationToken)
    {
        // Exercise the real ISchemaMapper: infer from the source's Arrow schema when
        // undeclared, else validate the source against the declaration.
        var arrow = ArrowSchemaBridge.ToArrow(_schema);
        if (declared is null)
        {
            return ValueTask.FromResult(ArrowSchemaMapper.Instance.Infer(arrow));
        }

        _ = ArrowSchemaMapper.Instance.Bind(declared, arrow); // throws if incompatible
        return ValueTask.FromResult(declared);
    }

    public ValueTask<IConnectorOffset?> LatestOffsetAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<IConnectorOffset?>(
            _versions.Count == 0 ? null : new LongOffset(_versions.Count - 1));

    public ValueTask<InputBatch?> NextAsync(IConnectorOffset from, CancellationToken cancellationToken)
    {
        var idx = ((LongOffset)from).Value + 1;
        if (idx >= _versions.Count)
        {
            return ValueTask.FromResult<InputBatch?>(null);
        }

        var version = _versions[(int)idx];
        var rows = new object?[version.Count][];
        var weights = new long[version.Count];
        for (var i = 0; i < version.Count; i++)
        {
            rows[i] = version[i].Row;
            weights[i] = version[i].Weight;
        }

        var batch = ArrowTestData.Batch(_schema, rows);
        var completed = idx == _versions.Count - 1;
        return ValueTask.FromResult<InputBatch?>(new InputBatch(batch, weights, new LongOffset(idx), completed));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>A spy sink: records each write's tick, row count and total weight so tests
/// can assert the write cadence and shape (the view's exact contents are checked via
/// <c>runner.Query.CurrentView</c>, the definitive source).</summary>
internal sealed class FakeOutputConnector : IOutputConnector
{
    private readonly List<(long Tick, int RowCount, long WeightSum)> _writes = new();

    public FakeOutputConnector(string viewName, OutputMode mode)
    {
        ViewName = viewName;
        Mode = mode;
    }

    public string ViewName { get; }

    public OutputMode Mode { get; }

    public SqlSchema? BoundSchema { get; private set; }

    public IReadOnlyList<(long Tick, int RowCount, long WeightSum)> Writes => _writes;

    public ValueTask BindSchemaAsync(SqlSchema viewSchema, CancellationToken cancellationToken)
    {
        BoundSchema = viewSchema;
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteViewAsync(RecordBatch view, long[] weights, long tick, CancellationToken cancellationToken)
    {
        _writes.Add((tick, view.Length, Sum(weights)));
        return ValueTask.CompletedTask;
    }

    public ValueTask WriteDeltaAsync(RecordBatch delta, long[] weights, long tick, CancellationToken cancellationToken)
    {
        _writes.Add((tick, delta.Length, Sum(weights)));
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static long Sum(long[] weights)
    {
        long s = 0;
        foreach (var w in weights)
        {
            s += w;
        }

        return s;
    }
}
