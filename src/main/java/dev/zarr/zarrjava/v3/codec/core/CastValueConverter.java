package dev.zarr.zarrjava.v3.codec.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec.OutOfRange;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec.Rounding;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * The pure value-casting math behind the {@link CastValueCodec}. This class knows nothing about the
 * Zarr codec pipeline; it only converts numeric values from one {@link DataType} to another following
 * the {@code cast_value} procedure (scalar_map, then exact representability, then rounding and
 * out-of-range handling). Keeping it separate from the codec makes the arithmetic easy to read and
 * test in isolation.
 *
 * <p>Values are carried through an exact {@link BigDecimal} / {@link BigInteger} intermediate so that
 * 64-bit integer exactness and directed rounding stay correct.
 */
final class CastValueConverter {

    private final Rounding rounding;
    private final OutOfRange outOfRange;

    CastValueConverter(Rounding rounding, OutOfRange outOfRange) {
        this.rounding = rounding;
        this.outOfRange = outOfRange;
    }

    // ===== Public API used by the codec ======================================================

    /**
     * Casts every element of {@code input} (a chunk in {@code inputType}) into a new array of
     * {@code outputType}.
     */
    Array castArray(Array input, DataType inputType, DataType outputType, List<ScalarEntry> entries)
            throws ZarrException {
        int[] shape = input.getShape();
        Array output = Array.factory(outputType.getMA2DataType(), shape);
        IndexIterator inputIt = input.getIndexIterator();
        IndexIterator outputIt = output.getIndexIterator();
        // Common casts (identity, float<->float, integer->integer) are handled with primitive
        // arithmetic to avoid the per-element BigDecimal/BigInteger allocation of the exact path.
        if (fastPathApplicable(inputType, outputType, !entries.isEmpty())) {
            castArrayFast(inputIt, outputIt, inputType, outputType);
            return output;
        }
        while (inputIt.hasNext()) {
            Scalar scalar = readScalar(inputIt, inputType);
            Object result = castScalar(scalar, outputType, entries);
            writeScalar(outputIt, outputType, result);
        }
        return output;
    }

    /**
     * Whether {@link #castArrayFast} can handle the given cast. Decided purely from the data types and
     * configuration (never the data) so that the choice is made once, before iterating. A fast path is
     * only used when it produces results identical to the exact path.
     */
    private boolean fastPathApplicable(DataType inputType, DataType outputType, boolean hasEntries) {
        if (hasEntries) {
            return false; // scalar_map matching needs the exact value model
        }
        if (inputType == outputType) {
            return true; // identity copy
        }
        if (isFloat(inputType) && isFloat(outputType)) {
            // Widening float32 -> float64 is exact; narrowing float64 -> float32 with round-to-nearest
            // -even matches a primitive Java narrowing cast. Directed rounding uses the exact path.
            return outputType == DataType.FLOAT64 || rounding == Rounding.NEAREST_EVEN;
        }
        if (isInteger(inputType) && isInteger(outputType)) {
            // uint64 does not fit a signed long, so leave those casts on the exact BigInteger path.
            return inputType != DataType.UINT64 && outputType != DataType.UINT64;
        }
        return false;
    }

    /** Primitive-arithmetic implementation of the casts accepted by {@link #fastPathApplicable}. */
    private void castArrayFast(IndexIterator inputIt, IndexIterator outputIt, DataType inputType,
                               DataType outputType) throws ZarrException {
        if (inputType == outputType) {
            while (inputIt.hasNext()) {
                copyElement(inputIt, outputIt, inputType);
            }
        } else if (isFloat(inputType)) {
            while (inputIt.hasNext()) {
                castFloatToFloatFast(inputIt, outputIt, inputType, outputType);
            }
        } else {
            while (inputIt.hasNext()) {
                castIntToIntFast(inputIt, outputIt, inputType, outputType);
            }
        }
    }

    private void castFloatToFloatFast(IndexIterator inputIt, IndexIterator outputIt, DataType inputType,
                                      DataType outputType) throws ZarrException {
        double value = inputType == DataType.FLOAT32 ? inputIt.getFloatNext() : inputIt.getDoubleNext();
        if (outputType == DataType.FLOAT64) {
            outputIt.setDoubleNext(value); // widening / identity: exact, sign and NaN preserved
            return;
        }
        float result = (float) value; // round-to-nearest-even, preserving NaN, +-Infinity and -0.0f
        if (Float.isInfinite(result) && !Double.isInfinite(value)) {
            // A finite value overflowed the float32 range.
            if (outOfRange != OutOfRange.CLAMP) {
                throw floatOverflowException(new BigDecimal(value), outputType);
            }
        }
        outputIt.setFloatNext(result);
    }

    private void castIntToIntFast(IndexIterator inputIt, IndexIterator outputIt, DataType inputType,
                                  DataType outputType) throws ZarrException {
        long value = readLong(inputIt, inputType);
        long min = integerMinLong(outputType);
        long max = integerMaxLong(outputType);
        long result;
        if (value >= min && value <= max) {
            result = value;
        } else if (outOfRange == null) {
            throw new ZarrException(
                    "The value '" + value + "' is out of range for the target data type '"
                            + outputType.getValue() + "' and no 'out_of_range' rule is configured.");
        } else if (outOfRange == OutOfRange.CLAMP) {
            result = value < min ? min : max;
        } else {
            // WRAP modulo 2^N. Only reached for out-of-range values, which never happens for a 64-bit
            // output, so the shift width here is always < 64.
            int bits = integerBits(outputType);
            long modulus = 1L << bits;
            long wrapped = ((value % modulus) + modulus) % modulus; // in [0, 2^N - 1]
            if (isSigned(outputType) && wrapped > max) {
                wrapped -= modulus;
            }
            result = wrapped;
        }
        writeLong(outputIt, outputType, result);
    }

    /** Casts a single (boxed) fill value into the boxed primitive of {@code outputType}. */
    Object castFillValue(Object fillValue, DataType inputType, DataType outputType,
                         List<ScalarEntry> entries) throws ZarrException {
        if (fillValue == null) {
            return null;
        }
        Object result = castScalar(scalarFromBoxed(fillValue, inputType), outputType, entries);
        return toBoxedForType(result, outputType);
    }

    /**
     * Returns whether a cast fill value decodes back to the original fill value. Used to validate that
     * the fill value survives a round-trip cast (a {@code cast_value} specification requirement).
     */
    boolean fillValueRoundTrips(Object castFillValue, Object originalFillValue, DataType outputType,
                                DataType inputType, List<ScalarEntry> decodeEntries)
            throws ZarrException {
        Object decoded = castScalar(scalarFromBoxed(castFillValue, outputType), inputType, decodeEntries);
        Object roundTripped = toBoxedForType(decoded, inputType);
        return scalarsEqual(scalarFromBoxed(roundTripped, inputType),
                scalarFromBoxed(originalFillValue, inputType));
    }

    /**
     * Turns a {@code scalar_map} JSON table into a list of parsed lookup entries. The input scalars are
     * parsed using {@code inputType}'s fill-value encoding, the output scalars using {@code outputType}'s.
     */
    List<ScalarEntry> buildEntries(Object[][] pairs, DataType inputType, DataType outputType)
            throws ZarrException {
        List<ScalarEntry> entries = new ArrayList<>();
        if (pairs == null) {
            return entries;
        }
        for (Object[] pair : pairs) {
            if (pair.length != 2) {
                throw new ZarrException("Each cast_value scalar_map entry must be a length-2 array.");
            }
            Object keyBoxed = ArrayMetadata.parseFillValue(pair[0], inputType);
            Object outputBoxed = ArrayMetadata.parseFillValue(pair[1], outputType);
            entries.add(new ScalarEntry(scalarFromBoxed(keyBoxed, inputType), outputBoxed));
        }
        return entries;
    }

    static void requireSupported(DataType type) throws ZarrException {
        if (!isFloat(type) && !isSigned(type) && !isUnsigned(type)) {
            throw new ZarrException(
                    "The cast_value codec does not support the data type '" + type.getValue()
                            + "'. Supported types are the integral and floating-point real-number types.");
        }
    }

    static boolean isFloatTarget(DataType type) {
        return isFloat(type);
    }

    // ===== The ordered casting procedure (one scalar) ========================================

    private Object castScalar(Scalar scalar, DataType outputType, List<ScalarEntry> entries)
            throws ZarrException {
        // 1. explicit scalar_map mapping (first match wins)
        for (ScalarEntry entry : entries) {
            if (scalarsEqual(entry.key, scalar)) {
                return entry.output;
            }
        }
        // 2. + 3. exact representability, then rounding and out-of-range handling
        if (isFloat(outputType)) {
            return castToFloat(scalar, outputType);
        }
        return castToInteger(scalar, outputType);
    }

    private Object castToInteger(Scalar scalar, DataType outputType) throws ZarrException {
        if (scalar.isNaN || scalar.isPositiveInfinity || scalar.isNegativeInfinity) {
            throw new ZarrException(
                    "Cannot cast a NaN or infinite value to the integer data type '" + outputType.getValue()
                            + "' without an explicit scalar_map mapping.");
        }
        BigInteger min = integerMin(outputType);
        BigInteger max = integerMax(outputType);
        BigDecimal value = scalar.value;

        BigInteger candidate;
        if (value.stripTrailingZeros().scale() <= 0) {
            candidate = value.toBigInteger(); // already an integer value; no rounding needed
        } else {
            candidate = value.setScale(0, rounding.mode).toBigInteger();
        }

        if (candidate.compareTo(min) >= 0 && candidate.compareTo(max) <= 0) {
            return candidate;
        }
        return handleOutOfRange(candidate, outputType, min, max);
    }

    private Object handleOutOfRange(BigInteger candidate, DataType outputType, BigInteger min,
                                    BigInteger max) throws ZarrException {
        if (outOfRange == null) {
            throw new ZarrException(
                    "The value '" + candidate + "' is out of range for the target data type '"
                            + outputType.getValue() + "' and no 'out_of_range' rule is configured.");
        }
        if (outOfRange == OutOfRange.CLAMP) {
            return candidate.compareTo(min) < 0 ? min : max;
        }
        // WRAP: map to the value congruent modulo 2^N inside the representable range.
        int bits = integerBits(outputType);
        BigInteger modulus = BigInteger.ONE.shiftLeft(bits);
        BigInteger wrapped = candidate.mod(modulus); // in [0, 2^N - 1]
        if (isSigned(outputType) && wrapped.compareTo(max) > 0) {
            wrapped = wrapped.subtract(modulus);
        }
        return wrapped;
    }

    private Object castToFloat(Scalar scalar, DataType outputType) throws ZarrException {
        boolean isFloat32 = outputType == DataType.FLOAT32;
        if (scalar.isNaN) {
            return isFloat32 ? (Object) Float.NaN : (Object) Double.NaN;
        }
        if (scalar.isPositiveInfinity) {
            return isFloat32 ? (Object) Float.POSITIVE_INFINITY : (Object) Double.POSITIVE_INFINITY;
        }
        if (scalar.isNegativeInfinity) {
            return isFloat32 ? (Object) Float.NEGATIVE_INFINITY : (Object) Double.NEGATIVE_INFINITY;
        }
        BigDecimal value = scalar.value;
        if (value.signum() == 0) {
            if (scalar.isNegativeZero) {
                return isFloat32 ? (Object) (-0.0f) : (Object) (-0.0d);
            }
            return isFloat32 ? (Object) 0.0f : (Object) 0.0d;
        }

        if (isFloat32) {
            float nearest = value.floatValue();
            if (!Float.isInfinite(nearest) && new BigDecimal((double) nearest).compareTo(value) == 0) {
                return nearest; // exactly representable
            }
            float result = roundToFloat(value);
            if (Float.isInfinite(result)) {
                return handleFloatOverflow(result, outputType, value);
            }
            return result;
        } else {
            double nearest = value.doubleValue();
            if (!Double.isInfinite(nearest) && new BigDecimal(nearest).compareTo(value) == 0) {
                return nearest; // exactly representable
            }
            double result = roundToDouble(value);
            if (Double.isInfinite(result)) {
                return handleFloatOverflow(result, outputType, value);
            }
            return result;
        }
    }

    private Object handleFloatOverflow(double infinite, DataType outputType, BigDecimal value)
            throws ZarrException {
        if (outOfRange == OutOfRange.CLAMP) {
            // Data types with +-Infinity map out-of-finite-range values to +-Infinity.
            return outputType == DataType.FLOAT32 ? (Object) (float) infinite : (Object) infinite;
        }
        throw floatOverflowException(value, outputType);
    }

    private static ZarrException floatOverflowException(BigDecimal value, DataType outputType) {
        return new ZarrException(
                "The value '" + value.toPlainString() + "' exceeds the finite range of the target data type '"
                        + outputType.getValue() + "' and no 'clamp' out_of_range rule is configured.");
    }

    /** Rounds an exact value to the nearest float allowed by the configured rounding mode. */
    private float roundToFloat(BigDecimal value) {
        float nearest = value.floatValue();
        if (Float.isInfinite(nearest)) {
            return nearest;
        }
        float down;
        float up;
        if (new BigDecimal((double) nearest).compareTo(value) > 0) {
            up = nearest;
            down = Math.nextDown(nearest);
        } else {
            down = nearest;
            up = Math.nextUp(nearest);
        }
        switch (rounding) {
            case TOWARDS_ZERO:
                return value.signum() > 0 ? down : up;
            case TOWARDS_POSITIVE:
                return up;
            case TOWARDS_NEGATIVE:
                return down;
            case NEAREST_AWAY:
                // Only differs from nearest-even at an exact tie, where ties go away from zero.
                if (value.subtract(new BigDecimal((double) down))
                        .compareTo(new BigDecimal((double) up).subtract(value)) == 0) {
                    return value.signum() > 0 ? up : down;
                }
                return nearest;
            default: // NEAREST_EVEN
                return nearest;
        }
    }

    private double roundToDouble(BigDecimal value) {
        double nearest = value.doubleValue();
        if (Double.isInfinite(nearest)) {
            return nearest;
        }
        double down;
        double up;
        if (new BigDecimal(nearest).compareTo(value) > 0) {
            up = nearest;
            down = Math.nextDown(nearest);
        } else {
            down = nearest;
            up = Math.nextUp(nearest);
        }
        switch (rounding) {
            case TOWARDS_ZERO:
                return value.signum() > 0 ? down : up;
            case TOWARDS_POSITIVE:
                return up;
            case TOWARDS_NEGATIVE:
                return down;
            case NEAREST_AWAY:
                // Only differs from nearest-even at an exact tie, where ties go away from zero.
                if (value.subtract(new BigDecimal(down))
                        .compareTo(new BigDecimal(up).subtract(value)) == 0) {
                    return value.signum() > 0 ? up : down;
                }
                return nearest;
            default: // NEAREST_EVEN
                return nearest;
        }
    }

    // ===== Reading / writing array elements ==================================================

    private static Scalar readScalar(IndexIterator it, DataType type) {
        switch (type) {
            case FLOAT32:
                return Scalar.ofFloat(it.getFloatNext());
            case FLOAT64:
                return Scalar.ofDouble(it.getDoubleNext());
            case INT8:
                return Scalar.ofInteger(BigInteger.valueOf(it.getByteNext()));
            case UINT8:
                return Scalar.ofInteger(BigInteger.valueOf(it.getByteNext() & 0xFFL));
            case INT16:
                return Scalar.ofInteger(BigInteger.valueOf(it.getShortNext()));
            case UINT16:
                return Scalar.ofInteger(BigInteger.valueOf(it.getShortNext() & 0xFFFFL));
            case INT32:
                return Scalar.ofInteger(BigInteger.valueOf(it.getIntNext()));
            case UINT32:
                return Scalar.ofInteger(BigInteger.valueOf(it.getIntNext() & 0xFFFFFFFFL));
            case INT64:
                return Scalar.ofInteger(BigInteger.valueOf(it.getLongNext()));
            case UINT64:
                return Scalar.ofInteger(new BigInteger(Long.toUnsignedString(it.getLongNext())));
            default:
                throw new IllegalStateException("Unsupported cast_value data type: " + type);
        }
    }

    private static void writeScalar(IndexIterator it, DataType type, Object value) {
        Number number = (Number) value;
        switch (type) {
            case FLOAT32:
                it.setFloatNext(number.floatValue());
                break;
            case FLOAT64:
                it.setDoubleNext(number.doubleValue());
                break;
            case INT8:
            case UINT8:
                it.setByteNext(number.byteValue());
                break;
            case INT16:
            case UINT16:
                it.setShortNext(number.shortValue());
                break;
            case INT32:
            case UINT32:
                it.setIntNext(number.intValue());
                break;
            case INT64:
            case UINT64:
                it.setLongNext(number.longValue());
                break;
            default:
                throw new IllegalStateException("Unsupported cast_value data type: " + type);
        }
    }

    /** Copies one element unchanged (used for identity casts in the fast path). */
    private static void copyElement(IndexIterator inputIt, IndexIterator outputIt, DataType type) {
        switch (type) {
            case FLOAT32:
                outputIt.setFloatNext(inputIt.getFloatNext());
                break;
            case FLOAT64:
                outputIt.setDoubleNext(inputIt.getDoubleNext());
                break;
            case INT8:
            case UINT8:
                outputIt.setByteNext(inputIt.getByteNext());
                break;
            case INT16:
            case UINT16:
                outputIt.setShortNext(inputIt.getShortNext());
                break;
            case INT32:
            case UINT32:
                outputIt.setIntNext(inputIt.getIntNext());
                break;
            case INT64:
            case UINT64:
                outputIt.setLongNext(inputIt.getLongNext());
                break;
            default:
                throw new IllegalStateException("Unsupported cast_value data type: " + type);
        }
    }

    /** Reads the next element as its exact signed value in a {@code long}. Not valid for uint64. */
    private static long readLong(IndexIterator it, DataType type) {
        switch (type) {
            case INT8:
                return it.getByteNext();
            case UINT8:
                return it.getByteNext() & 0xFFL;
            case INT16:
                return it.getShortNext();
            case UINT16:
                return it.getShortNext() & 0xFFFFL;
            case INT32:
                return it.getIntNext();
            case UINT32:
                return it.getIntNext() & 0xFFFFFFFFL;
            case INT64:
                return it.getLongNext();
            default:
                throw new IllegalStateException("readLong is not valid for cast_value data type: " + type);
        }
    }

    /** Writes {@code value} into the next element, truncating to the type's width. Not valid for uint64. */
    private static void writeLong(IndexIterator it, DataType type, long value) {
        switch (type) {
            case INT8:
            case UINT8:
                it.setByteNext((byte) value);
                break;
            case INT16:
            case UINT16:
                it.setShortNext((short) value);
                break;
            case INT32:
            case UINT32:
                it.setIntNext((int) value);
                break;
            case INT64:
                it.setLongNext(value);
                break;
            default:
                throw new IllegalStateException("writeLong is not valid for cast_value data type: " + type);
        }
    }

    /** Reconstructs a {@link Scalar} from a boxed value (a fill value or a scalar_map key). */
    private static Scalar scalarFromBoxed(Object value, DataType type) {
        Number number = (Number) value;
        switch (type) {
            case FLOAT32:
                return Scalar.ofFloat(number.floatValue());
            case FLOAT64:
                return Scalar.ofDouble(number.doubleValue());
            case INT8:
                return Scalar.ofInteger(BigInteger.valueOf(number.byteValue()));
            case UINT8:
                return Scalar.ofInteger(BigInteger.valueOf(number.longValue() & 0xFFL));
            case INT16:
                return Scalar.ofInteger(BigInteger.valueOf(number.shortValue()));
            case UINT16:
                return Scalar.ofInteger(BigInteger.valueOf(number.longValue() & 0xFFFFL));
            case INT32:
                return Scalar.ofInteger(BigInteger.valueOf(number.intValue()));
            case UINT32:
                return Scalar.ofInteger(BigInteger.valueOf(number.longValue() & 0xFFFFFFFFL));
            case INT64:
                return Scalar.ofInteger(BigInteger.valueOf(number.longValue()));
            case UINT64:
                return Scalar.ofInteger(new BigInteger(Long.toUnsignedString(number.longValue())));
            default:
                throw new IllegalStateException("Unsupported cast_value data type: " + type);
        }
    }

    /**
     * Converts a cast result (a {@link BigInteger} for integral targets, or a boxed float/double) into
     * the exact boxed primitive type expected for a fill value of {@code type}.
     */
    private static Object toBoxedForType(Object value, DataType type) {
        Number number = (Number) value;
        switch (type) {
            case FLOAT32:
                return number.floatValue();
            case FLOAT64:
                return number.doubleValue();
            case INT8:
            case UINT8:
                return number.byteValue();
            case INT16:
            case UINT16:
                return number.shortValue();
            case INT32:
            case UINT32:
                return number.intValue();
            case INT64:
            case UINT64:
                return number.longValue();
            default:
                throw new IllegalStateException("Unsupported cast_value data type: " + type);
        }
    }

    // ===== Value model & comparison ==========================================================

    private static boolean scalarsEqual(Scalar a, Scalar b) {
        if (a.isNaN || b.isNaN) {
            return a.isNaN && b.isNaN;
        }
        if (a.isPositiveInfinity || b.isPositiveInfinity) {
            return a.isPositiveInfinity && b.isPositiveInfinity;
        }
        if (a.isNegativeInfinity || b.isNegativeInfinity) {
            return a.isNegativeInfinity && b.isNegativeInfinity;
        }
        return a.value.compareTo(b.value) == 0;
    }

    /** An exact numeric value, or one of the special IEEE-754 values (NaN, +-Infinity). */
    private static final class Scalar {
        final boolean isNaN;
        final boolean isPositiveInfinity;
        final boolean isNegativeInfinity;
        final boolean isNegativeZero;
        final BigDecimal value; // null for the special values above

        private Scalar(boolean isNaN, boolean isPositiveInfinity, boolean isNegativeInfinity,
                       boolean isNegativeZero, BigDecimal value) {
            this.isNaN = isNaN;
            this.isPositiveInfinity = isPositiveInfinity;
            this.isNegativeInfinity = isNegativeInfinity;
            this.isNegativeZero = isNegativeZero;
            this.value = value;
        }

        static Scalar ofDouble(double d) {
            if (Double.isNaN(d)) {
                return new Scalar(true, false, false, false, null);
            }
            if (d == Double.POSITIVE_INFINITY) {
                return new Scalar(false, true, false, false, null);
            }
            if (d == Double.NEGATIVE_INFINITY) {
                return new Scalar(false, false, true, false, null);
            }
            boolean negZero = (d == 0.0d) && (Double.doubleToRawLongBits(d) != 0L);
            return new Scalar(false, false, false, negZero, new BigDecimal(d));
        }

        static Scalar ofFloat(float f) {
            if (Float.isNaN(f)) {
                return new Scalar(true, false, false, false, null);
            }
            if (f == Float.POSITIVE_INFINITY) {
                return new Scalar(false, true, false, false, null);
            }
            if (f == Float.NEGATIVE_INFINITY) {
                return new Scalar(false, false, true, false, null);
            }
            boolean negZero = (f == 0.0f) && (Float.floatToRawIntBits(f) != 0);
            return new Scalar(false, false, false, negZero, new BigDecimal((double) f));
        }

        static Scalar ofInteger(BigInteger i) {
            return new Scalar(false, false, false, false, new BigDecimal(i));
        }
    }

    /** A single {@code scalar_map} lookup entry: match {@link #key}, emit {@link #output}. */
    static final class ScalarEntry {
        final Scalar key;
        final Object output; // boxed primitive of the output data type

        private ScalarEntry(Scalar key, Object output) {
            this.key = key;
            this.output = output;
        }
    }

    // ===== Data type facts ===================================================================

    private static boolean isFloat(DataType type) {
        return type == DataType.FLOAT32 || type == DataType.FLOAT64;
    }

    private static boolean isSigned(DataType type) {
        return type == DataType.INT8 || type == DataType.INT16 || type == DataType.INT32
                || type == DataType.INT64;
    }

    private static boolean isUnsigned(DataType type) {
        return type == DataType.UINT8 || type == DataType.UINT16 || type == DataType.UINT32
                || type == DataType.UINT64;
    }

    private static boolean isInteger(DataType type) {
        return isSigned(type) || isUnsigned(type);
    }

    private static int integerBits(DataType type) {
        return type.getByteCount() * 8;
    }

    /** The minimum representable value as a {@code long}. Not valid for uint64. */
    private static long integerMinLong(DataType type) {
        return isUnsigned(type) ? 0L : -(1L << (integerBits(type) - 1));
    }

    /** The maximum representable value as a {@code long}. Not valid for uint64. */
    private static long integerMaxLong(DataType type) {
        if (isUnsigned(type)) {
            return (1L << integerBits(type)) - 1;
        }
        return (1L << (integerBits(type) - 1)) - 1;
    }

    private static BigInteger integerMin(DataType type) {
        if (isUnsigned(type)) {
            return BigInteger.ZERO;
        }
        return BigInteger.ONE.shiftLeft(integerBits(type) - 1).negate();
    }

    private static BigInteger integerMax(DataType type) {
        if (isUnsigned(type)) {
            return BigInteger.ONE.shiftLeft(integerBits(type)).subtract(BigInteger.ONE);
        }
        return BigInteger.ONE.shiftLeft(integerBits(type) - 1).subtract(BigInteger.ONE);
    }
}
