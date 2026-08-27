package dev.zarr.zarrjava.utils;

/**
 * Conversions between IEEE 754 binary16 ("half precision") bit patterns and Java {@code float}.
 *
 * <p>Zarr's {@code float16} data type stores 2 bytes per element, but there is no 16-bit floating
 * point primitive in Java and no half precision type in {@code ucar.ma2.DataType}. Half precision
 * arrays are therefore held in memory as {@code float} (binary32) and converted at the codec
 * boundary. Widening binary16 to binary32 is always exact; narrowing is not, see
 * {@link #floatToHalfBits(float)}.
 *
 * <p>{@code Float.float16ToFloat} and {@code Float.floatToFloat16} would do this for us, but they
 * were added in Java 20 and this project targets Java 8, so the conversions are implemented here.
 * The results match {@code numpy.float16} for all inputs, including subnormals, signed zeros,
 * infinities, NaN payloads and round-half-to-even ties.
 */
public final class Float16 {

    /**
     * Largest finite binary16 value, {@code 65504.0}. Finite {@code float} values greater than
     * {@code MAX_VALUE + 16} round to infinity when narrowed.
     */
    public static final float MAX_VALUE = 65504.0f;

    /** Smallest positive normal binary16 value, {@code 2^-14}. */
    public static final float MIN_NORMAL = 6.103515625E-5f;

    /** Smallest positive subnormal binary16 value, {@code 2^-24}. */
    public static final float MIN_VALUE = 5.9604645E-8f;

    private Float16() {
    }

    /**
     * Widens a binary16 bit pattern to the {@code float} of equal value. Exact for every input:
     * every binary16 value is representable as a binary32.
     *
     * <p>Signed zeros and infinities are preserved. NaN payloads are shifted into the binary32
     * mantissa, so a NaN stays a NaN rather than becoming an infinity.
     *
     * @param halfBits the 16 bits of a binary16 value, in the low half of the short
     * @return the equal-valued float
     */
    public static float halfBitsToFloat(short halfBits) {
        final int h = halfBits & 0xFFFF;
        final int signBit = (h >>> 15) & 0x1;
        final int exponent = (h >>> 10) & 0x1F;
        int mantissa = h & 0x3FF;

        final int floatSign = signBit << 31;

        if (exponent == 0) {
            if (mantissa == 0) {
                // Signed zero.
                return Float.intBitsToFloat(floatSign);
            }
            // Subnormal binary16, which is normal as a binary32: shift the mantissa up until the
            // implicit leading bit is set, decrementing the exponent for each shift.
            int normalizedExponent = 1;
            while ((mantissa & 0x400) == 0) {
                mantissa <<= 1;
                normalizedExponent--;
            }
            mantissa &= 0x3FF; // Drop the leading bit; it is implicit in binary32 too.
            return Float.intBitsToFloat(
                    floatSign | ((normalizedExponent - 15 + 127) << 23) | (mantissa << 13));
        }

        if (exponent == 0x1F) {
            // Infinity (mantissa 0) or NaN (mantissa non-zero); shifting keeps the payload non-zero.
            return Float.intBitsToFloat(floatSign | 0x7F800000 | (mantissa << 13));
        }

        // Normal binary16.
        return Float.intBitsToFloat(floatSign | ((exponent - 15 + 127) << 23) | (mantissa << 13));
    }

    /**
     * Narrows a {@code float} to the nearest binary16 bit pattern, rounding to nearest with ties to
     * even -- the same rounding {@code numpy.float16} and IEEE 754 use.
     *
     * <p>This conversion is lossy. binary16 carries 11 bits of significand against binary32's 24,
     * and its finite range stops at {@value #MAX_VALUE}:
     * <ul>
     *   <li>Values at or above {@code 65520.0} become infinity, not {@link #MAX_VALUE}. That
     *       threshold is the midpoint between {@link #MAX_VALUE} and the next power of two, and it
     *       rounds up because ties go to even. Writing {@code 1e8} to a {@code float16} array
     *       stores infinity.</li>
     *   <li>Values below {@code 2^-25} become signed zero; values between {@code 2^-25} and
     *       {@link #MIN_NORMAL} lose precision to the subnormal range.</li>
     *   <li>Other values are rounded to 11 significant bits, so {@code 0.1} stores as
     *       {@code 0.0999755859375}.</li>
     * </ul>
     * Callers writing data they cannot afford to round should not be using {@code float16}.
     *
     * @param value the float to narrow
     * @return the 16 bits of the nearest binary16 value, in the low half of the short
     */
    public static short floatToHalfBits(float value) {
        final int f = Float.floatToRawIntBits(value);
        final int sign = (f >>> 16) & 0x8000;
        final int exponent = (f >>> 23) & 0xFF;
        final int mantissa = f & 0x7FFFFF;

        if (exponent == 0xFF) {
            if (mantissa == 0) {
                return (short) (sign | 0x7C00); // Infinity.
            }
            // NaN. Keep the top payload bits, but never let the payload round down to zero, which
            // would silently turn the NaN into an infinity.
            int payload = mantissa >>> 13;
            if (payload == 0) {
                payload = 1;
            }
            return (short) (sign | 0x7C00 | payload);
        }

        final int unbiasedExponent = exponent - 127;

        if (unbiasedExponent > 15) {
            return (short) (sign | 0x7C00); // Overflows the binary16 range.
        }

        if (unbiasedExponent >= -14) {
            // Normal binary16. Keep the top 10 mantissa bits and round on the remaining 13.
            final int halfMantissa = mantissa >>> 13;
            final int roundBits = mantissa & 0x1FFF;
            final int bits = sign | ((unbiasedExponent + 15) << 10) | halfMantissa;
            if (roundBits > 0x1000 || (roundBits == 0x1000 && (halfMantissa & 1) != 0)) {
                // A carry out of the mantissa increments the exponent, and a carry out of the
                // maximum exponent yields infinity. Both are the correct IEEE 754 results.
                return (short) (bits + 1);
            }
            return (short) bits;
        }

        if (unbiasedExponent >= -25) {
            // Subnormal binary16, or a value small enough that rounding may still reach the
            // smallest subnormal. Restore the implicit leading bit, then shift into place so that
            // the result is interpreted as halfMantissa * 2^-24.
            final int significand = mantissa | 0x800000;
            final int shift = -unbiasedExponent - 1;
            final int halfMantissa = significand >>> shift;
            final int roundBits = significand & ((1 << shift) - 1);
            final int tie = 1 << (shift - 1);
            if (roundBits > tie || (roundBits == tie && (halfMantissa & 1) != 0)) {
                return (short) (sign | (halfMantissa + 1));
            }
            return (short) (sign | halfMantissa);
        }

        // Underflows to signed zero: strictly less than half of the smallest subnormal.
        return (short) sign;
    }
}
