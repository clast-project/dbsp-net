// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Reflection;
using System.Runtime.Loader;
using DbspNet.Persistence;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;
using DbspNet.Sql.TypeSystem;

namespace DbspNet.Tests.Persistence;

/// <summary>
/// A typed-path snapshot must restore in a process other than the one that wrote it.
/// The plan fingerprint hashes operator <see cref="Type.FullName"/>s, whose generic
/// arguments name the emitted row structs; when those names were random GUIDs, every
/// typed snapshot failed "plan fingerprint mismatch" outside its writer process, and
/// in-process round-trip tests could not see it (the emitter's type cache hid it).
/// A fresh <see cref="AssemblyLoadContext"/> loads its own copy of the DbspNet
/// assemblies — fresh emitter statics, freshly emitted types — standing in for the
/// second process.
/// </summary>
public sealed class TypedSnapshotCrossContextTests : IDisposable
{
    private const string Ddl = "CREATE TABLE t (g INT NOT NULL, v BIGINT NOT NULL, s VARCHAR)";

    private const string Query =
        "SELECT g, MAX(v) AS mx, COUNT(*) AS n, COUNT(s) AS ns FROM t GROUP BY g";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "dbspnet-xctx-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, recursive: true);
        }
    }

    [Fact]
    public async Task SnapshotWrittenInAnotherLoadContext_RestoresAndContinues()
    {
        // Emit an unrelated row type first, so this context's emission order differs
        // from the writer context's (a counter-based name would also fail here).
        _ = TypedRowEmitter.EmitRowType(new Schema([new SchemaColumn("z", new SqlDoubleType(true))]));

        // Not collectible: a collectible context holding a second copy of the engine
        // slowed the whole test run from ~17 s to ~13 min.
        var alc = new IsolatedContext(typeof(TypedSnapshotCrossContextTests).Assembly.Location);
        var asm = alc.LoadFromAssemblyPath(typeof(TypedSnapshotCrossContextTests).Assembly.Location);
        var writer = asm.GetType(typeof(TypedSnapshotCrossContextTests).FullName!)!
            .GetMethod(nameof(WriteSnapshot), BindingFlags.NonPublic | BindingFlags.Static)!;
        Assert.NotSame(typeof(TypedSnapshotCrossContextTests).Assembly, asm);
        writer.Invoke(null, [_dir]);

        var reference = Compile();
        Drive(reference, Before);
        var restored = Compile();
        await Snapshot.ReadAsync(restored.Circuit, _dir);

        reference.Table("t").Insert(After);
        restored.Table("t").Insert(After);
        reference.Step();
        restored.Step();
        Assert.Equal(Delta(reference), Delta(restored));
        Assert.NotEmpty(Delta(restored));
    }

    /// <summary>Runs inside the isolated context: build, drive, snapshot.</summary>
    internal static void WriteSnapshot(string dir)
    {
        var q = Compile();
        Assert.Contains(q.Circuit.Operators, o => o.GetType().FullName!.Contains("TypedRow_", StringComparison.Ordinal));
        Drive(q, Before);
        Snapshot.WriteAsync(q.Circuit, dir).AsTask().GetAwaiter().GetResult();
    }

    private static readonly object?[][] Before =
    [
        [1, 10L, "a"], [1, 30L, null], [2, 5L, "b"], [3, 7L, "c"], [3, 9L, "d"],
    ];

    // Retracts group 1's MAX and adds to group 2: needs the restored state to answer.
    private static readonly object?[] After = [2, 50L, "e"];

    private static void Drive(CompiledQuery q, object?[][] rows)
    {
        foreach (var r in rows)
        {
            q.Table("t").Insert(r);
        }

        q.Step();
        q.Table("t").Delete(rows[1]);
        q.Step();
    }

    private static CompiledQuery Compile()
    {
        var resolver = new Resolver(new Catalog());
        resolver.Resolve(Parser.ParseStatement(Ddl));
        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(Query))).Query;
        return PlanToCircuit.Compile(plan, ArrowSqlSnapshotCodecs.Instance);
    }

    private static List<string> Delta(CompiledQuery q)
        => q.Current
            .Select(e => string.Join("|", Enumerable.Range(0, e.Key.Count).Select(i => e.Key[i]?.ToString() ?? "NULL")) + "#" + e.Value.Value)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

    /// <summary>Loads its own copy of every DbspNet assembly; everything else is shared.</summary>
    private sealed class IsolatedContext(string mainAssemblyPath) : AssemblyLoadContext(isCollectible: false)
    {
        private readonly AssemblyDependencyResolver _resolver = new(mainAssemblyPath);

        protected override Assembly? Load(AssemblyName name)
        {
            if (name.Name is null || !name.Name.StartsWith("DbspNet", StringComparison.Ordinal))
            {
                return null;
            }

            var path = _resolver.ResolveAssemblyToPath(name);
            return path is null ? null : LoadFromAssemblyPath(path);
        }
    }
}
