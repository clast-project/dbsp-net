namespace DbspNet.Core.Algebra;

/// <summary>
/// The canonical 64-bit integer weight. Wraps <see cref="long"/> to provide
/// an <see cref="IZRing{TSelf}"/> instance via static abstract members.
/// Arithmetic is <c>checked</c> — overflow throws rather than wrapping silently.
/// </summary>
public readonly record struct Z64(long Value) : IZRing<Z64>
{
    public static Z64 Zero => default;

    public static Z64 One => new(1);

    public static Z64 Add(Z64 a, Z64 b) => new(checked(a.Value + b.Value));

    public static Z64 Negate(Z64 a) => new(checked(-a.Value));

    public static Z64 Subtract(Z64 a, Z64 b) => new(checked(a.Value - b.Value));

    public static Z64 Multiply(Z64 a, Z64 b) => new(checked(a.Value * b.Value));

    public static bool IsPositive(Z64 a) => a.Value > 0;

    public static bool IsZero(Z64 a) => a.Value == 0;

    public static Z64 operator +(Z64 a, Z64 b) => Add(a, b);

    public static Z64 operator -(Z64 a, Z64 b) => Subtract(a, b);

    public static Z64 operator -(Z64 a) => Negate(a);

    public static Z64 operator *(Z64 a, Z64 b) => Multiply(a, b);

    public static implicit operator Z64(long value) => new(value);

    public static implicit operator long(Z64 value) => value.Value;

    public override string ToString() => Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
}
