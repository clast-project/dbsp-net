// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Circuit;

/// <summary>
/// Declares that no type in <see cref="Namespace"/> (or a namespace nested in it) of the marked
/// assembly can reach a circuit stream, so the stream-release reader scan
/// (<see cref="StreamRelease"/>) does not walk into its objects. Only for data built before any
/// circuit exists (a query plan): the scan is conservative, and a wrong declaration would release a
/// stream before a reader that the scan no longer sees.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly, AllowMultiple = true)]
internal sealed class StreamFreeNamespaceAttribute(string ns) : Attribute
{
    /// <summary>The stream-free namespace.</summary>
    public string Namespace { get; } = ns;
}
