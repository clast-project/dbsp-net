using Apache.Arrow;
using Clast.DatabaseDecimal.Values;
using DbspNet.Arrow;
using DbspNet.Sql.Compiler;
using DbspNet.Sql.Parser;
using DbspNet.Sql.Plan;

namespace DbspNet.Tests.Arrow;

/// <summary>
/// Round-trip tests for the zero-copy ingest path. The Arrow batch is kept
/// alive for the lifetime of the test scope so the engine's aliased
/// <see cref="Utf8String"/> references stay valid.
/// </summary>
public class ArrowZeroCopyTests
{
    private static CompiledQuery Compile(string[] ddl, string query)
    {
        var catalog = new Catalog();
        var resolver = new Resolver(catalog);
        foreach (var s in ddl)
        {
            resolver.Resolve(Parser.ParseStatement(s));
        }

        var plan = ((SelectPlan)resolver.Resolve(Parser.ParseStatement(query))).Query;
        return PlanToCircuit.Compile(plan);
    }

    [Fact]
    public void PushArrowZeroCopy_AsciiStrings_RoundTrip()
    {
        var q = Compile(
            ["CREATE TABLE t (id INT NOT NULL, name VARCHAR NOT NULL)"],
            "SELECT id, name FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var idBuilder = new Int32Array.Builder();
        idBuilder.Append(1).Append(2).Append(3);
        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append("alice").Append("bob").Append("carol");
        var batch = new RecordBatch(arrowSchema, new IArrowArray[]
        {
            idBuilder.Build(),
            nameBuilder.Build(),
        }, length: 3);

        q.Table("t").PushArrowZeroCopy(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        Assert.Equal(3, delta.Rows.Length);

        var nameCol = (StringArray)delta.Rows.Column(1);
        var roundTripped = new HashSet<string>();
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            roundTripped.Add(nameCol.GetString(i));
        }

        Assert.Contains("alice", roundTripped);
        Assert.Contains("bob", roundTripped);
        Assert.Contains("carol", roundTripped);

        // Keep batch reachable until after we've consumed delta — the engine's
        // aliased rows would otherwise risk a managed-buffer GC during ToArrowDelta
        // if the batch wasn't rooted by this local. (For managed-array buffers,
        // the GC keeps them alive via the Utf8String's ReadOnlyMemory anyway.)
        GC.KeepAlive(batch);
    }

    [Fact]
    public void PushArrowZeroCopy_MultibyteStrings_PreserveBytes()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t");

        var arrowSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var sb = new StringArray.Builder();
        sb.Append("café").Append("⚡").Append("🎉");
        var batch = new RecordBatch(arrowSchema, new IArrowArray[] { sb.Build() }, 3);

        q.Table("t").PushArrowZeroCopy(batch);
        q.Step();

        var delta = q.ToArrowDelta();
        var col = (StringArray)delta.Rows.Column(0);
        var values = new HashSet<string>();
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            values.Add(col.GetString(i));
        }

        Assert.Contains("café", values);
        Assert.Contains("⚡", values);
        Assert.Contains("🎉", values);

        GC.KeepAlive(batch);
    }

    [Fact]
    public void PushArrowZeroCopy_MixedWeights_AppliesInsertsAndRetractions()
    {
        var q = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t");

        // Initial state: alice, bob.
        var setupSchema = ArrowSchemaBridge.ToArrow(q.Table("t").Schema);
        var initBuilder = new StringArray.Builder();
        initBuilder.Append("alice").Append("bob");
        var initBatch = new RecordBatch(setupSchema, new IArrowArray[] { initBuilder.Build() }, 2);
        q.Table("t").PushArrowZeroCopy(initBatch);
        q.Step();
        Assert.Equal(2, q.Current.Count);

        // Retract alice and insert charlie in one batch.
        var deltaBuilder = new StringArray.Builder();
        deltaBuilder.Append("alice").Append("charlie");
        var deltaBatch = new RecordBatch(setupSchema, new IArrowArray[] { deltaBuilder.Build() }, 2);
        long[] weights = { -1, 1 };
        q.Table("t").PushArrowZeroCopy(deltaBatch, weights);
        q.Step();

        // Reading the delta confirms both ops applied.
        var delta = q.ToArrowDelta();
        var col = (StringArray)delta.Rows.Column(0);
        for (var i = 0; i < delta.Rows.Length; i++)
        {
            var name = col.GetString(i);
            if (name == "alice")
            {
                Assert.Equal(-1, delta.Weights[i]);
            }
            else
            {
                Assert.Equal("charlie", name);
                Assert.Equal(1, delta.Weights[i]);
            }
        }

        GC.KeepAlive(initBatch);
        GC.KeepAlive(deltaBatch);
    }

    [Fact]
    public void PushArrowZeroCopy_CopyAndZeroCopy_ProduceIdenticalState()
    {
        // Push the same data through both paths; verify the engine's
        // observable state is identical.
        var q1 = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t");
        var q2 = Compile(
            ["CREATE TABLE t (s VARCHAR NOT NULL)"],
            "SELECT s FROM t");

        var schema = ArrowSchemaBridge.ToArrow(q1.Table("t").Schema);
        var sb = new StringArray.Builder();
        sb.Append("apple").Append("banana").Append("cherry");
        var batch = new RecordBatch(schema, new IArrowArray[] { sb.Build() }, 3);

        q1.Table("t").PushArrow(batch);          // copy
        q2.Table("t").PushArrowZeroCopy(batch);  // alias
        q1.Step();
        q2.Step();

        var d1 = q1.ToArrowDelta();
        var d2 = q2.ToArrowDelta();

        Assert.Equal(d1.Rows.Length, d2.Rows.Length);
        var s1 = new HashSet<string>();
        var s2 = new HashSet<string>();
        var col1 = (StringArray)d1.Rows.Column(0);
        var col2 = (StringArray)d2.Rows.Column(0);
        for (var i = 0; i < d1.Rows.Length; i++)
        {
            s1.Add(col1.GetString(i));
            s2.Add(col2.GetString(i));
        }

        Assert.Equal(s1, s2);
        GC.KeepAlive(batch);
    }
}
