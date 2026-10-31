// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
// The workers of a parallel snapshot must load and write CONCURRENTLY even over a file system
// whose calls complete synchronously (LocalTableFileSystem does). Before the fix,
// ParallelSnapshot.ReadAsync called each worker's load in a plain loop, so with a synchronous file
// system the workers restored one after another: 19.0 s at W=12 on ivm-bench SF=3 against 8.7 s
// serial (docs/design-durable-state.md §1b). Timing would make a flaky test; a rendezvous does
// not. Each worker's first operator-file access blocks until every worker has arrived, so a
// sequential walk times out on worker 0.
//
// The restore was sequential only when each worker's share of Snapshot.DefaultRestoreParallelism
// came to 1, i.e. W above half the default degree (W=12 against 7). With a larger share,
// LoadDirAsync took its Parallel.ForEachAsync branch, which does go to the pool, which is why the
// W=2..4 tests never saw it. The read test therefore uses W=9: the default is capped at 8, so the
// share is 1 on any machine.
using DbspNet.Core.IO;
using DbspNet.Persistence;
using DbspNet.Persistence.IO;
using DbspNet.Sql.Compiler;

namespace DbspNet.Tests.Persistence;

public class ParallelSnapshotConcurrencyTests
{
    private const int Workers = 3;

    private const int ReadWorkers = 9;

    private static ParallelCompiledProgram Compile(int workers = Workers)
    {
        Assert.True(
            SqlProgram.TryCompileParallel(
                [
                    "CREATE TABLE t (k INT NOT NULL, v BIGINT NOT NULL)",
                    "CREATE VIEW agg AS SELECT k, SUM(v) AS s FROM t GROUP BY k",
                ],
                new HashSet<string>(["agg"], StringComparer.Ordinal),
                workers,
                out var p,
                ArrowSqlSnapshotCodecs.Instance),
            "parallel compile refused");
        for (var k = 0; k < 30; k++)
        {
            p!.Table("t").Insert(k, (long)k);
        }

        p!.Step();
        return p;
    }

    [Fact]
    public async Task Write_RunsTheWorkersConcurrently_OverASynchronousFileSystem()
    {
        using var fs = new RendezvousFileSystem(Workers);
        using var p = Compile();

        await ParallelSnapshot.WriteAsync(p.Circuit, fs);

        Assert.True(fs.AllArrived, "a worker's write waited for the others in vain: the workers ran one after another");
    }

    [Fact]
    public async Task Read_RunsTheWorkersConcurrently_OverASynchronousFileSystem()
    {
        Assert.Equal(1, Math.Max(1, Snapshot.DefaultRestoreParallelism / ReadWorkers));
        using var fs = new RendezvousFileSystem(ReadWorkers);
        using (var producer = Compile(ReadWorkers))
        {
            fs.Armed = false;
            await ParallelSnapshot.WriteAsync(producer.Circuit, fs);
        }

        fs.Arm();
        using var restored = Compile(ReadWorkers);
        await ParallelSnapshot.ReadAsync(restored.Circuit, fs);

        Assert.True(fs.AllArrived, "a worker's restore waited for the others in vain: the workers ran one after another");
    }

    /// <summary>In-memory, synchronous like the local file system. The first operator-file access
    /// under each <c>worker-{w}/</c> blocks, on the calling thread, until all workers have made
    /// theirs or a timeout passes.</summary>
    private sealed class RendezvousFileSystem(int workers) : ITableFileSystem, IDisposable
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
        private readonly InMemoryTableFileSystem _inner = new();
        private readonly object _gate = new();
        private HashSet<int> _seen = [];
        private CountdownEvent _arrivals = new(workers);
        private bool _timedOut;

        public bool Armed { get; set; } = true;

        public bool AllArrived => !_timedOut && _arrivals.IsSet;

        public void Arm()
        {
            lock (_gate)
            {
                _seen = [];
                _arrivals.Dispose();
                _arrivals = new CountdownEvent(workers);
                _timedOut = false;
                Armed = true;
            }
        }

        public void Dispose() => _arrivals.Dispose();

        private void Rendezvous(string path)
        {
            if (!Armed || !path.Contains("/op-", StringComparison.Ordinal))
            {
                return;
            }

            var at = path.IndexOf("worker-", StringComparison.Ordinal);
            if (at < 0)
            {
                return;
            }

            var end = path.IndexOf('/', at);
            var worker = int.Parse(path.AsSpan(at + "worker-".Length, end - at - "worker-".Length), System.Globalization.CultureInfo.InvariantCulture);
            CountdownEvent arrivals;
            lock (_gate)
            {
                if (_timedOut || !_seen.Add(worker))
                {
                    return;
                }

                arrivals = _arrivals;
            }

            arrivals.Signal();
            if (!arrivals.Wait(Timeout))
            {
                lock (_gate)
                {
                    _timedOut = true;
                }
            }
        }

        public IAsyncEnumerable<TableFileInfo> ListAsync(string prefix, CancellationToken cancellationToken = default) =>
            _inner.ListAsync(prefix, cancellationToken);

        public ValueTask<IRandomAccessFile> OpenReadAsync(string path, CancellationToken cancellationToken = default)
        {
            Rendezvous(path);
            return _inner.OpenReadAsync(path, cancellationToken);
        }

        public ValueTask<ISequentialFile> CreateAsync(string path, bool overwrite = false, CancellationToken cancellationToken = default)
        {
            Rendezvous(path);
            return _inner.CreateAsync(path, overwrite, cancellationToken);
        }

        public ValueTask<bool> RenameAsync(string sourcePath, string targetPath, CancellationToken cancellationToken = default) =>
            _inner.RenameAsync(sourcePath, targetPath, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            _inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            _inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken = default)
        {
            Rendezvous(path);
            return _inner.ReadAllBytesAsync(path, cancellationToken);
        }

        public ValueTask WriteAllBytesAsync(string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Rendezvous(path);
            return _inner.WriteAllBytesAsync(path, data, cancellationToken);
        }
    }
}
