// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
namespace DbspNet.Core.Collections;

/// <summary>
/// An explicit "value or absent" wrapper that behaves uniformly across value
/// types and reference types — unlike <see cref="Nullable{T}"/> (structs only)
/// and annotated nullable references (reference types only). Used by aggregate
/// results (empty groups → <see cref="None"/>) and, in Phase 5+, by SQL NULL.
/// </summary>
public readonly struct Optional<T> : IEquatable<Optional<T>>
    where T : notnull
{
    private readonly T _value;

    public Optional(T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        _value = value;
        HasValue = true;
    }

    public bool HasValue { get; }

    public T Value =>
        HasValue ? _value : throw new InvalidOperationException("Optional is None.");

    public T GetValueOr(T fallback) => HasValue ? _value : fallback;

    public static Optional<T> None => default;

    public static Optional<T> Some(T value) => new(value);

    public bool Equals(Optional<T> other)
    {
        if (HasValue != other.HasValue)
        {
            return false;
        }

        return !HasValue || EqualityComparer<T>.Default.Equals(_value, other._value);
    }

    public override bool Equals(object? obj) => obj is Optional<T> other && Equals(other);

    public override int GetHashCode() => HasValue ? HashCode.Combine(true, _value) : 0;

    public static bool operator ==(Optional<T> left, Optional<T> right) => left.Equals(right);

    public static bool operator !=(Optional<T> left, Optional<T> right) => !left.Equals(right);

    public override string ToString() => HasValue ? (_value.ToString() ?? "NULL") : "NULL";
}
