// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Implemented by an operator that replaced an older operator type while keeping
/// that operator's snapshot data format. Restore computes a second plan
/// fingerprint with <see cref="FingerprintAliasType"/> in place of the operator's
/// own type, so snapshots written before the replacement still load.
/// </summary>
internal interface IFingerprintAlias
{
    /// <summary>The operator type whose snapshots this operator can restore.</summary>
    Type FingerprintAliasType { get; }
}

/// <summary>
/// An operator whose snapshot is the same as another operator type's, file for file, in both
/// directions: it fingerprints as <see cref="FingerprintType"/> when a snapshot is written and when
/// one is read, so either can restore the other's (unlike <see cref="IFingerprintAlias"/>, which
/// only lets this operator read the other's). The block-held inner join
/// (<c>BlockedJoinOp</c>, docs/design-flat-trace-state.md §3) is the managed join's twin.
/// </summary>
internal interface IFingerprintIdentity
{
    /// <summary>The operator type this operator's snapshots are indistinguishable from.</summary>
    Type FingerprintType { get; }
}
