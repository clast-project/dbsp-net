// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Runtime.CompilerServices;

namespace Clast.BloomFilter;

internal static class SplitMix64
{
    /// <summary>
    /// David Stafford's Mix13 finalizer. Maps any 64-bit input to a
    /// near-uniform 64-bit output; suitable as a cheap hash mixer
    /// when the input already has good low-bit entropy.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Mix(ulong x)
    {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9UL;
        x ^= x >> 27;
        x *= 0x94d049bb133111ebUL;
        x ^= x >> 31;
        return x;
    }
}
