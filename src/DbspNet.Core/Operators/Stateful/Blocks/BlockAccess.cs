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

    public void Touch()
    {
        var e = BlockAccess.Epoch;
        if (_epoch != e)
        {
            _epoch = e;
            _bits = 0;
        }

        _bits |= BlockAccess.CurrentBits;
    }

    /// <summary>The phase bits read in <paramref name="epoch"/> (0: not read in it).</summary>
    public readonly int BitsIn(int epoch) => _epoch == epoch ? _bits : 0;
}
