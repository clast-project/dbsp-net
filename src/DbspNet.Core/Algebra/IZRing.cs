// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Algebra;

/// <summary>
/// A Z-ring: a ring whose elements can be classified as positive, negative or
/// zero. The classification is used by <c>distinct</c> (emit on crossings of
/// zero) and to cull zero-weight entries from Z-sets.
/// </summary>
public interface IZRing<TSelf> : IRingValue<TSelf>
    where TSelf : IZRing<TSelf>
{
    /// <summary>Returns true iff <paramref name="a"/> is strictly positive.</summary>
    static abstract bool IsPositive(TSelf a);

    /// <summary>Returns true iff <paramref name="a"/> equals <c>Zero</c>.</summary>
    static abstract bool IsZero(TSelf a);
}
