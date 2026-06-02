// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using System.Numerics;

namespace DbspNet.Core.Operators.Stateful.Aggregators;

/// <summary>
/// A HyperLogLog cardinality sketch — the bounded-state estimator behind
/// SQL <c>APPROX_COUNT_DISTINCT</c>. Hashes are folded in with
/// <see cref="AddHash"/>; the distinct-value estimate is read back with
/// <see cref="EstimateCardinality"/>.
/// </summary>
/// <remarks>
/// <para><b>Determinism.</b> The register array is a pure function of the
/// <i>set</i> of hashes folded in — <see cref="AddHash"/> is idempotent
/// (register holds a max) and order-independent. So two runs that fold the
/// same set of distinct values, in any order and with any repetition,
/// produce byte-identical registers and therefore an identical estimate.
/// That is what lets the incremental aggregator match a from-scratch batch
/// recompute <i>exactly</i> (not merely within tolerance): both end at the
/// same present-value set, hence the same sketch.</para>
/// <para><b>State size.</b> <c>2^precision</c> bytes — one register per
/// bucket, independent of cardinality. The default
/// <see cref="DefaultPrecision"/> of 12 gives 4096 registers (4&#160;KiB) and
/// a standard error of ~1.6% in the estimation regime; below ~2.5·m distinct
/// values the linear-counting correction takes over and is near-exact.</para>
/// <para>64-bit hashes are assumed, so the classic 32-bit large-range
/// correction is unnecessary and omitted (matching the HLL++ guidance).</para>
/// </remarks>
public sealed class HyperLogLog
{
    /// <summary>Default register-index bit width (m = 2^12 = 4096 registers).</summary>
    public const int DefaultPrecision = 12;

    private readonly int _precision;
    private readonly int _registerCount;
    private readonly byte[] _registers;
    private readonly int _maxRho;

    public HyperLogLog(int precision = DefaultPrecision)
    {
        // p in [4, 18] keeps m between 16 and 262144 — the useful range and
        // the band the alpha constants below are defined over.
        if (precision is < 4 or > 18)
        {
            throw new ArgumentOutOfRangeException(
                nameof(precision), precision, "HyperLogLog precision must be in [4, 18].");
        }

        _precision = precision;
        _registerCount = 1 << precision;
        _registers = new byte[_registerCount];

        // The suffix that feeds rho is (64 - p) bits, so the largest possible
        // run of leading zeros is (64 - p), giving a maximum rho of (64-p)+1.
        _maxRho = 64 - precision + 1;
    }

    /// <summary>The register-index bit width this sketch was built with.</summary>
    public int Precision => _precision;

    /// <summary>The number of registers (<c>2^Precision</c>).</summary>
    public int RegisterCount => _registerCount;

    /// <summary>Reset every register to zero, reusing the backing array.</summary>
    public void Clear() => Array.Clear(_registers);

    /// <summary>
    /// Fold one value's 64-bit hash into the sketch. The top
    /// <see cref="Precision"/> bits select the register; the position of the
    /// leftmost set bit in the remaining suffix (1-based) is the candidate
    /// rank, and the register keeps the running maximum. Idempotent: folding
    /// the same hash again never changes the registers.
    /// </summary>
    public void AddHash(ulong hash)
    {
        var index = (int)(hash >> (64 - _precision));

        // Shift the index bits off the top; the (64-p) suffix bits now occupy
        // the high end (low p bits are zero). LeadingZeroCount of the result is
        // the suffix's leading-zero run, except the all-zero suffix yields 64 —
        // clamped to _maxRho below.
        var suffix = hash << _precision;
        var rho = BitOperations.LeadingZeroCount(suffix) + 1;
        if (rho > _maxRho)
        {
            rho = _maxRho;
        }

        if (rho > _registers[index])
        {
            _registers[index] = (byte)rho;
        }
    }

    /// <summary>
    /// Estimate the number of distinct hashes folded in. Uses the raw
    /// HyperLogLog estimator with the small-range (linear-counting)
    /// correction; an empty sketch estimates 0.
    /// </summary>
    public long EstimateCardinality()
    {
        var m = (double)_registerCount;

        double inverseSum = 0;
        var zeroRegisters = 0;
        foreach (var register in _registers)
        {
            // 2^-register; register fits well within the exponent range.
            inverseSum += 1.0 / (1UL << register);
            if (register == 0)
            {
                zeroRegisters++;
            }
        }

        var alpha = Alpha(_registerCount);
        var estimate = alpha * m * m / inverseSum;

        // Small-range correction: when the raw estimate is small and some
        // registers are still empty, linear counting (V = empty registers) is
        // far more accurate than the harmonic-mean estimator.
        if (estimate <= 2.5 * m && zeroRegisters > 0)
        {
            estimate = m * Math.Log(m / zeroRegisters);
        }

        return (long)Math.Round(estimate, MidpointRounding.AwayFromZero);
    }

    private static double Alpha(int m) => m switch
    {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m),
    };
}
