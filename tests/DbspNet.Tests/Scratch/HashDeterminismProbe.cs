// Which of our hash paths are process-stable? Prints, so two runs can be diffed.
// Gated on DBSPNET_HASH_PROBE=1.
using System.IO.Hashing;
using DbspNet.Core.Circuit;
using DbspNet.Core.Collections;
using DbspNet.Sql.TypeSystem;
using Xunit;
using Xunit.Abstractions;

namespace DbspNet.Tests.Scratch;

public class HashDeterminismProbe
{
    private readonly ITestOutputHelper _out;

    public HashDeterminismProbe(ITestOutputHelper output) => _out = output;

    [Fact]
    public void PrintHashes()
    {
        if (Environment.GetEnvironmentVariable("DBSPNET_HASH_PROBE") is not "1")
        {
            return;
        }

        var u = Utf8String.Of("ACME");
        object boxed = u;
        _out.WriteLine($"Utf8String.GetHashCode      {u.GetHashCode()}");
        _out.WriteLine($"boxed Utf8String            {boxed.GetHashCode()}");
        _out.WriteLine($"string.GetHashCode          {"ACME".GetHashCode()}");
        _out.WriteLine($"XxHash3 of bytes            {XxHash3.HashToUInt64(u.Span)}");
        _out.WriteLine($"StableHash.Of(string)       {StableHash.Of("ACME")}");
        _out.WriteLine($"HashCode.Combine(1,2)       {HashCode.Combine(1, 2)}");
        _out.WriteLine($"double 1.5                  {1.5.GetHashCode()}");
        _out.WriteLine($"decimal 1.5m                {1.5m.GetHashCode()}");
        _out.WriteLine($"long 42                     {42L.GetHashCode()}");
        _out.WriteLine($"DateTime(2020,1,1)          {new DateTime(2020, 1, 1).GetHashCode()}");
        _out.WriteLine($"StructuralRow(long,Utf8)    {new StructuralRow(42L, u).GetHashCode()}");
        _out.WriteLine($"StructuralRow(long,long)    {new StructuralRow(42L, 7L).GetHashCode()}");
    }
}
