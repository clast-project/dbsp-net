// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Runtime.CompilerServices;
using DbspNet.Arrow;
using DbspNet.Connectors.Abstractions;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;
using SqlSchema = DbspNet.Sql.Plan.Schema;

namespace DbspNet.Connectors.EngineeredWood;

/// <summary>
/// A bounded <see cref="IInputConnector"/> over a single Parquet file, backed by
/// engineered-wood. The whole file is delivered once as inserts (a single version, one
/// tick) — useful for one-shot loads and tests. Offset is <c>Before</c> → <c>0</c>
/// (read / not-read).
/// </summary>
public sealed class ParquetInputConnector : IInputConnector
{
    private readonly ITableFileSystem _fs;
    private readonly string _fileKey;
    private readonly ISchemaMapper _mapper;

    private SqlSchema? _schema;
    private ArrowSchema? _resolvedArrow;

    public ParquetInputConnector(string name, ITableFileSystem fs, string fileKey, ISchemaMapper? mapper = null)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        _fs = fs ?? throw new ArgumentNullException(nameof(fs));
        _fileKey = fileKey ?? throw new ArgumentNullException(nameof(fileKey));
        _mapper = mapper ?? ArrowSchemaMapper.Instance;
    }

    public ParquetInputConnector(string name, string directory, string fileName, ISchemaMapper? mapper = null)
        : this(name, new LocalTableFileSystem(directory), fileName, mapper)
    {
    }

    public string Name { get; }

    public IConnectorOffset InitialOffset => LongOffset.Before;

    public IConnectorOffset ParseOffset(string serialized) => LongOffset.Parse(serialized);

    public async ValueTask<SqlSchema> ResolveSchemaAsync(SqlSchema? declared, CancellationToken cancellationToken)
    {
        // The reader derives the Arrow schema per read; peek the first batch's schema.
        ArrowSchema? sourceArrow = null;
        var file = await _fs.OpenReadAsync(_fileKey, cancellationToken).ConfigureAwait(false);
        await using (var reader = new ParquetFileReader(file))
        {
            await foreach (var batch in reader.ReadAllAsync(null, cancellationToken).ConfigureAwait(false))
            {
                sourceArrow = batch.Schema;
                break;
            }
        }

        if (sourceArrow is null)
        {
            _schema = declared ?? throw new InvalidOperationException(
                $"cannot infer a schema from empty Parquet file '{_fileKey}' — declare the table");
        }
        else if (declared is null)
        {
            _schema = _mapper.Infer(sourceArrow);
        }
        else
        {
            _ = _mapper.Bind(declared, sourceArrow); // validate
            _schema = declared;
        }

        _resolvedArrow = ArrowSchemaBridge.ToArrow(_schema);
        return _schema;
    }

    // A single "version": the whole file. Latest offset is 0; once consumed the cursor
    // is at 0 and there is nothing more.
    public ValueTask<IConnectorOffset?> LatestOffsetAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<IConnectorOffset?>(new LongOffset(0));

    public ValueTask<InputBatch?> NextAsync(IConnectorOffset from, CancellationToken cancellationToken)
    {
        if (((LongOffset)from).Value >= 0)
        {
            return ValueTask.FromResult<InputBatch?>(null); // already read
        }

        return ValueTask.FromResult<InputBatch?>(
            new InputBatch(StreamFile(cancellationToken), new LongOffset(0), Completed: true));
    }

    private async IAsyncEnumerable<VersionBatch> StreamFile([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var file = await _fs.OpenReadAsync(_fileKey, cancellationToken).ConfigureAwait(false);
        await using var reader = new ParquetFileReader(file);
        await foreach (var raw in reader.ReadAllAsync(null, cancellationToken).ConfigureAwait(false))
        {
            yield return ArrowProjection.Project(raw, _schema!, _resolvedArrow!, Name);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
