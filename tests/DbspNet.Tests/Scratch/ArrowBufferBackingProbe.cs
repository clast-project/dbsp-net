// Is an Arrow IPC read buffer backed by a managed array or by native memory? Decides two things
// for zero-copy VARCHAR on the restore path (docs/design-incremental-persistence.md §11.4b):
// whether aliasing survives disposing the batch, and whether Utf8String.Span is a cheap array
// slice or a virtual MemoryManager call — the suspect for that experiment's +152% hash cost.
// Gated on DBSPNET_ARROW_BACKING_PROBE=1.
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class ArrowBufferBackingProbe
{
    private readonly ITestOutputHelper _out;

    public ArrowBufferBackingProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void ReportBacking()
    {
        if (Environment.GetEnvironmentVariable("DBSPNET_ARROW_BACKING_PROBE") is not "1")
        {
            return;
        }

        var schema = new Schema(new[] { new Field("s", Apache.Arrow.Types.StringType.Default, true) }, null);
        var b = new StringArray.Builder();
        for (var i = 0; i < 1000; i++)
        {
            b.Append("SYMBOL" + i);
        }

        using var batch = new RecordBatch(schema, new IArrowArray[] { b.Build() }, 1000);
        using var ms = new MemoryStream();
        using (var w = new ArrowStreamWriter(ms, schema, leaveOpen: true))
        {
            w.WriteRecordBatch(batch);
            w.WriteEnd();
        }

        ms.Position = 0;
        using var reader = new ArrowStreamReader(ms, leaveOpen: true);
        var read = reader.ReadNextRecordBatch()!;
        var arr = (StringArray)read.Column(0);

        var mem = arr.ValueBuffer.Memory;
        var isArray = MemoryMarshal.TryGetArray(mem, out var seg);
        _out.WriteLine($"value buffer length      {mem.Length}");
        _out.WriteLine($"backed by managed array  {isArray} (offset {seg.Offset}, count {seg.Count})");

        // Does the data survive disposing the batch? If native-backed, this is a use-after-free and
        // the bytes may or may not still read correctly — which is exactly why it must not ship.
        var before = mem.Span[0];
        read.Dispose();
        _out.WriteLine($"first byte before dispose {before}, after dispose {mem.Span[0]}");
    }
}
