// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Operators.Stateful.Blocks;

/// <summary>
/// Which blocks a batch reads (docs/design-bounded-memory.md §1, the access census): with
/// <c>DBSPNET_BLOCK_ACCESS=1</c> every block read stamps the block with the current
/// <see cref="Epoch"/> and ORs in the current <see cref="Phase"/> (and <see cref="Maintenance"/> on
/// a thread merging runs). A census then counts the blocks stamped this epoch. Off by default and
/// free when off (a static readonly flag the JIT folds).
/// </summary>
internal static class BlockAccess
{
    public static readonly bool Enabled = Environment.GetEnvironmentVariable("DBSPNET_BLOCK_ACCESS") == "1";

    public const int Drain = 1;
    public const int Write = 2;
    public const int Checkpoint = 4;
    public const int Restore = 8;
    public const int Maintenance = 16;

    /// <summary>The batch (or other window) a census reports on; a block whose stamp is older was not
    /// read in it.</summary>
    public static volatile int Epoch = 1;

    /// <summary>What the process is doing now (<see cref="Drain"/>, <see cref="Write"/>, …), set by
    /// the harness.</summary>
    public static volatile int Phase = Drain;

    [ThreadStatic]
    private static bool _maintenance;

    /// <summary>Whether this thread is merging or freezing runs (reads that rewrite, not probe).</summary>
    public static bool InMaintenance
    {
        get => _maintenance;
        set => _maintenance = value;
    }

    /// <summary>With <c>DBSPNET_BLOCK_ACCESS_STACKS=&lt;phase bits&gt;</c>: the first read of a block in
    /// one of those phases (per epoch) records its caller, grouped by the innermost DbspNet frames
    /// — who reads what (§1.2 item 4).</summary>
    public static readonly int StackPhases = int.TryParse(Environment.GetEnvironmentVariable("DBSPNET_BLOCK_ACCESS_STACKS"), out var p) ? p : 0;

    private static readonly Dictionary<string, (long Blocks, long Bytes)> Stacks = new();

    public static void RecordStack(long bytes)
    {
        var frames = new System.Diagnostics.StackTrace(2, false).GetFrames();
        var sb = new System.Text.StringBuilder();
        var n = 0;
        foreach (var f in frames)
        {
            var m = f.GetMethod();
            var t = m?.DeclaringType;
            if (t?.Namespace is not { } ns || !ns.StartsWith("DbspNet", StringComparison.Ordinal) || t.Name.Contains("TraceBlock", StringComparison.Ordinal))
            {
                continue;
            }

            sb.Append(n == 0 ? "" : " < ").Append(t.Name).Append('.').Append(m!.Name);
            if (++n == 7)
            {
                break;
            }
        }

        var key = sb.ToString();
        lock (Stacks)
        {
            Stacks[key] = Stacks.TryGetValue(key, out var v) ? (v.Blocks + 1, v.Bytes + bytes) : (1, bytes);
        }
    }

    /// <summary>The recorded callers, most bytes first; clears them.</summary>
    public static List<(string Stack, long Blocks, long Bytes)> TakeStacks()
    {
        lock (Stacks)
        {
            var list = Stacks.Select(kv => (kv.Key, kv.Value.Blocks, kv.Value.Bytes)).OrderByDescending(x => x.Bytes).ToList();
            Stacks.Clear();
            return list;
        }
    }

    /// <summary>
    /// Whether a hash spanning blocks gets blocks of its own, its neighbours cut off
    /// (docs/design-bounded-memory.md §3.5–§3.6). It only pays when blocks can be evicted — a probe
    /// for a neighbour would otherwise fault in the giant group — and costs the first checkpoint
    /// ~5–9% (more, smaller blocks), so it follows the block cache: on with
    /// <c>DBSPNET_BLOCK_CACHE_MIB</c>; <c>DBSPNET_SPAN_ALIGN=on|off</c> overrides.
    /// </summary>
    public static bool AlignSpans { get; set; } = Environment.GetEnvironmentVariable("DBSPNET_SPAN_ALIGN") switch
    {
        "on" => true,
        "off" => false,
        _ => long.TryParse(Environment.GetEnvironmentVariable("DBSPNET_BLOCK_CACHE_MIB"), out var mib) && mib > 0,
    };

    /// <summary>Whether a block cache can evict (set by the cache's budget): state with no run file
    /// then writes a backing one at checkpoints (§3.7).</summary>
    public static bool Evicting { get; set; } =
        long.TryParse(Environment.GetEnvironmentVariable("DBSPNET_BLOCK_CACHE_MIB"), out var budget) && budget > 0;

    /// <summary>The bits a read now records: <see cref="Maintenance"/> alone on a merging thread,
    /// else the phase.</summary>
    public static int CurrentBits => _maintenance ? Maintenance : Phase;
}

/// <summary>A block's last-read stamp (see <see cref="BlockAccess"/>).</summary>
internal struct BlockStamp
{
    private int _epoch;
    private int _bits;
    private int _born;

    /// <summary>Record the epoch the block was built (or adopted) in.</summary>
    public void Born() => _born = BlockAccess.Epoch;

    /// <summary>The epoch the block was built in.</summary>
    public readonly int BornIn => _born;

    /// <summary>Stamp a read; true when it is the block's first read this epoch in a phase
    /// <see cref="BlockAccess.StackPhases"/> asks callers for.</summary>
    public bool Touch()
    {
        var e = BlockAccess.Epoch;
        if (_epoch != e)
        {
            _epoch = e;
            _bits = 0;
        }

        var bits = BlockAccess.CurrentBits;
        var fresh = (bits & ~_bits & BlockAccess.StackPhases) != 0;
        _bits |= bits;
        return fresh;
    }

    /// <summary>The phase bits read in <paramref name="epoch"/> (0: not read in it).</summary>
    public readonly int BitsIn(int epoch) => _epoch == epoch ? _bits : 0;
}
