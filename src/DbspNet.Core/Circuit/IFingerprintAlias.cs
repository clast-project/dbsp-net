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
