package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata.CoreArrayMetadata;
import dev.zarr.zarrjava.core.codec.ArrayArrayCodec;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.Codec;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;

/**
 * The {@code scale_offset} codec applies the affine transformation {@code (in - offset) * scale} to
 * every array element on encode, and inverts it with {@code (in / scale) + offset} on decode. It is
 * an {@code array -> array} codec: it does not change the data type or shape, it only rescales the
 * stored values. It is typically followed by a narrowing codec (e.g. {@code cast_value}) that
 * converts the rescaled array to a smaller data type to achieve (lossy) compression.
 *
 * <p>The arithmetic is performed using the semantics of the input array's data type. For integral
 * data types the computation is exact: if any intermediate or final value is not representable in
 * the data type (e.g. an unsigned subtraction going negative, an overflow, or a non-exact division
 * on decode), the codec fails with a {@link ZarrException}. For floating-point data types the
 * computation uses native {@code float}/{@code double} arithmetic, whose results are always
 * representable (including {@code NaN} and {@code +-Infinity}).
 *
 * <p>The {@code offset} and {@code scale} configuration values are scalars encoded with the input
 * array's data type using the Zarr V3 fill value encoding. A missing {@code offset} defaults to the
 * additive identity (0); a missing {@code scale} defaults to the multiplicative identity (1). When
 * both are absent the codec is a no-op.
 *
 * <p>Supported data types are the real-number types this library models: {@code int8/16/32/64},
 * {@code uint8/16/32/64}, {@code float32} and {@code float64}. Other data types from the codec
 * specification (e.g. {@code float8_*}, {@code bfloat16}, {@code int2}) are not modelled here.
 */
public class ScaleOffsetCodec extends ArrayArrayCodec implements Codec {

    @JsonIgnore
    @Nonnull
    public final String name = "scale_offset";
    @Nullable
    public final Configuration configuration;

    @JsonCreator
    public ScaleOffsetCodec(
            @Nullable @JsonProperty(value = "configuration") Configuration configuration
    ) {
        this.configuration = configuration;
    }

    // ===== Codec pipeline integration ========================================================

    @Override
    public Array encode(Array chunkArray) throws ZarrException {
        return transform(chunkArray, true);
    }

    @Override
    public Array decode(Array chunkArray) throws ZarrException {
        return transform(chunkArray, false);
    }

    @Override
    public long computeEncodedSize(long inputByteLength, ArrayMetadata.CoreArrayMetadata arrayMetadata)
            throws ZarrException {
        // The data type and shape are unchanged, so the encoded chunk has the same byte length.
        return inputByteLength;
    }

    @Override
    public CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        super.resolveArrayMetadata();
        DataType type = arrayDataType();
        requireSupported(type);
        // The data type stays the same; only the fill value is transformed (encode direction) so that
        // fill-value-aware downstream codecs stay aligned with the rescaled data.
        Object transformedFillValue = transformFillValue(arrayMetadata.parsedFillValue, type);
        return new CoreArrayMetadata(
                arrayMetadata.shape, arrayMetadata.chunkShape, type, transformedFillValue);
    }

    private DataType arrayDataType() throws ZarrException {
        if (!(arrayMetadata.dataType instanceof DataType)) {
            throw new ZarrException("The scale_offset codec requires a Zarr v3 data type.");
        }
        return (DataType) arrayMetadata.dataType;
    }

    // ===== Element transformation ============================================================

    private Array transform(Array input, boolean encode) throws ZarrException {
        DataType type = arrayDataType();
        requireSupported(type);
        int[] shape = input.getShape();
        Array output = Array.factory(type.getMA2DataType(), shape);
        IndexIterator in = input.getIndexIterator();
        IndexIterator out = output.getIndexIterator();

        if (type == DataType.FLOAT32) {
            float offset = floatParam(offsetConfig(), 0.0f);
            float scale = floatParam(scaleConfig(), 1.0f);
            while (in.hasNext()) {
                float x = in.getFloatNext();
                out.setFloatNext(encode ? (x - offset) * scale : (x / scale) + offset);
            }
        } else if (type == DataType.FLOAT64) {
            double offset = doubleParam(offsetConfig(), 0.0);
            double scale = doubleParam(scaleConfig(), 1.0);
            while (in.hasNext()) {
                double x = in.getDoubleNext();
                out.setDoubleNext(encode ? (x - offset) * scale : (x / scale) + offset);
            }
        } else {
            BigInteger offset = intParam(offsetConfig(), BigInteger.ZERO, type);
            BigInteger scale = intParam(scaleConfig(), BigInteger.ONE, type);
            BigInteger min = integerMin(type);
            BigInteger max = integerMax(type);
            while (in.hasNext()) {
                BigInteger x = readInt(in, type);
                BigInteger r = encode
                        ? encodeInt(x, offset, scale, type, min, max)
                        : decodeInt(x, offset, scale, type, min, max);
                writeInt(out, type, r);
            }
        }
        return output;
    }

    private Object transformFillValue(Object fillValue, DataType type) throws ZarrException {
        if (fillValue == null) {
            return null;
        }
        if (type == DataType.FLOAT32) {
            float offset = floatParam(offsetConfig(), 0.0f);
            float scale = floatParam(scaleConfig(), 1.0f);
            float x = ((Number) fillValue).floatValue();
            return (x - offset) * scale;
        }
        if (type == DataType.FLOAT64) {
            double offset = doubleParam(offsetConfig(), 0.0);
            double scale = doubleParam(scaleConfig(), 1.0);
            double x = ((Number) fillValue).doubleValue();
            return (x - offset) * scale;
        }
        BigInteger offset = intParam(offsetConfig(), BigInteger.ZERO, type);
        BigInteger scale = intParam(scaleConfig(), BigInteger.ONE, type);
        BigInteger r = encodeInt(toBigInteger(fillValue, type), offset, scale, type,
                integerMin(type), integerMax(type));
        return boxInt(r, type);
    }

    // ===== Integer arithmetic (exact, with representability checks) ==========================

    private static BigInteger encodeInt(BigInteger x, BigInteger offset, BigInteger scale,
                                        DataType type, BigInteger min, BigInteger max)
            throws ZarrException {
        BigInteger shifted = x.subtract(offset);
        requireInRange(shifted, min, max, type, "intermediate value (in - offset)");
        BigInteger scaled = shifted.multiply(scale);
        requireInRange(scaled, min, max, type, "result (in - offset) * scale");
        return scaled;
    }

    private static BigInteger decodeInt(BigInteger x, BigInteger offset, BigInteger scale,
                                        DataType type, BigInteger min, BigInteger max)
            throws ZarrException {
        if (scale.signum() == 0) {
            throw new ZarrException("The scale_offset codec cannot decode with a scale of 0.");
        }
        BigInteger[] quotientRemainder = x.divideAndRemainder(scale);
        if (quotientRemainder[1].signum() != 0) {
            throw new ZarrException(
                    "The scale_offset codec cannot decode the value " + x + " because it is not exactly "
                            + "divisible by the scale " + scale + " in the '" + type.getValue() + "' data type.");
        }
        BigInteger divided = quotientRemainder[0];
        requireInRange(divided, min, max, type, "intermediate value (in / scale)");
        BigInteger result = divided.add(offset);
        requireInRange(result, min, max, type, "result (in / scale) + offset");
        return result;
    }

    private static void requireInRange(BigInteger value, BigInteger min, BigInteger max,
                                       DataType type, String label) throws ZarrException {
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
            throw new ZarrException(
                    "The scale_offset " + label + " (" + value + ") is not representable in the '"
                            + type.getValue() + "' data type.");
        }
    }

    // ===== Configuration parameter parsing ===================================================

    @Nullable
    private Object offsetConfig() {
        return configuration == null ? null : configuration.offset;
    }

    @Nullable
    private Object scaleConfig() {
        return configuration == null ? null : configuration.scale;
    }

    private static float floatParam(@Nullable Object raw, float identity) throws ZarrException {
        if (raw == null) {
            return identity;
        }
        return ((Number) ArrayMetadata.parseFillValue(raw, DataType.FLOAT32)).floatValue();
    }

    private static double doubleParam(@Nullable Object raw, double identity) throws ZarrException {
        if (raw == null) {
            return identity;
        }
        return ((Number) ArrayMetadata.parseFillValue(raw, DataType.FLOAT64)).doubleValue();
    }

    private static BigInteger intParam(@Nullable Object raw, BigInteger identity, DataType type)
            throws ZarrException {
        if (raw == null) {
            return identity;
        }
        return toBigInteger(ArrayMetadata.parseFillValue(raw, type), type);
    }

    // ===== Data type facts and element reading/writing =======================================

    private static void requireSupported(DataType type) throws ZarrException {
        if (type == DataType.BOOL) {
            throw new ZarrException(
                    "The scale_offset codec does not support the data type '" + type.getValue()
                            + "'. Supported types are the integral and floating-point real-number types.");
        }
    }

    private static BigInteger readInt(IndexIterator it, DataType type) {
        switch (type) {
            case INT8:
                return BigInteger.valueOf(it.getByteNext());
            case UINT8:
                return BigInteger.valueOf(it.getByteNext() & 0xFFL);
            case INT16:
                return BigInteger.valueOf(it.getShortNext());
            case UINT16:
                return BigInteger.valueOf(it.getShortNext() & 0xFFFFL);
            case INT32:
                return BigInteger.valueOf(it.getIntNext());
            case UINT32:
                return BigInteger.valueOf(it.getIntNext() & 0xFFFFFFFFL);
            case INT64:
                return BigInteger.valueOf(it.getLongNext());
            case UINT64:
                return new BigInteger(Long.toUnsignedString(it.getLongNext()));
            default:
                throw new IllegalStateException("Unsupported scale_offset data type: " + type);
        }
    }

    private static void writeInt(IndexIterator it, DataType type, BigInteger value) {
        switch (type) {
            case INT8:
            case UINT8:
                it.setByteNext(value.byteValue());
                break;
            case INT16:
            case UINT16:
                it.setShortNext(value.shortValue());
                break;
            case INT32:
            case UINT32:
                it.setIntNext(value.intValue());
                break;
            case INT64:
            case UINT64:
                it.setLongNext(value.longValue());
                break;
            default:
                throw new IllegalStateException("Unsupported scale_offset data type: " + type);
        }
    }

    private static BigInteger toBigInteger(Object boxed, DataType type) {
        Number number = (Number) boxed;
        switch (type) {
            case INT8:
                return BigInteger.valueOf(number.byteValue());
            case UINT8:
                return BigInteger.valueOf(number.longValue() & 0xFFL);
            case INT16:
                return BigInteger.valueOf(number.shortValue());
            case UINT16:
                return BigInteger.valueOf(number.longValue() & 0xFFFFL);
            case INT32:
                return BigInteger.valueOf(number.intValue());
            case UINT32:
                return BigInteger.valueOf(number.longValue() & 0xFFFFFFFFL);
            case INT64:
                return BigInteger.valueOf(number.longValue());
            case UINT64:
                return new BigInteger(Long.toUnsignedString(number.longValue()));
            default:
                throw new IllegalStateException("Unsupported scale_offset data type: " + type);
        }
    }

    private static Object boxInt(BigInteger value, DataType type) {
        switch (type) {
            case INT8:
            case UINT8:
                return value.byteValue();
            case INT16:
            case UINT16:
                return value.shortValue();
            case INT32:
            case UINT32:
                return value.intValue();
            case INT64:
            case UINT64:
                return value.longValue();
            default:
                throw new IllegalStateException("Unsupported scale_offset data type: " + type);
        }
    }

    private static int integerBits(DataType type) {
        return type.getByteCount() * 8;
    }

    private static boolean isUnsigned(DataType type) {
        return type == DataType.UINT8 || type == DataType.UINT16 || type == DataType.UINT32
                || type == DataType.UINT64;
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

    // ===== Configuration =====================================================================

    public static final class Configuration {

        /** The offset subtracted on encode, as a JSON scalar in the input array's data type. */
        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("offset")
        public final Object offset;

        /** The scale multiplied on encode, as a JSON scalar in the input array's data type. */
        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("scale")
        public final Object scale;

        @JsonCreator
        public Configuration(
                @Nullable @JsonProperty("offset") Object offset,
                @Nullable @JsonProperty("scale") Object scale) {
            this.offset = offset;
            this.scale = scale;
        }
    }
}
