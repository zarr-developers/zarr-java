package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.scalableminds.zfpjava.Zfp;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;
import ucar.ma2.Array;
import ucar.ma2.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Compresses chunks with the <a href="https://zfp.io">zfp</a> algorithm, as specified by the
 * <a href="https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/zfp">zfp codec
 * extension</a>.
 *
 * <p>zfp itself only handles 1- to 4-dimensional arrays of {@code int32}, {@code int64},
 * {@code float32} and {@code float64}. Narrower integer data types are promoted to {@code int32} and
 * demoted back with the shifts given in the specification; unsigned 32- and 64-bit values are
 * clamped into the signed range, matching the reference implementation in zarrs. Clamping is lossy
 * for values above {@code 2^31 - 1} respectively {@code 2^63 - 1}, even in reversible mode.
 */
public class ZfpCodec extends ArrayBytesCodec implements Codec {

    @JsonIgnore
    @Nonnull
    public final String name = "zfp";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZfpCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) {
        this.configuration = configuration;
    }

    private static int clamp(int value, int min, int max) {
        return Math.min(Math.max(value, min), max);
    }

    @Override
    public ByteBuffer encode(Array chunkArray) throws ZarrException {
        final DataType dataType = arrayMetadata.dataType.getMA2DataType();
        final Zfp.Type zfpType = zfpType(dataType);
        final int[] zfpShape = zfpShape();

        final ByteBuffer promoted =
                ByteBuffer.allocate((int) Zfp.countValues(zfpShape) * zfpType.getByteCount())
                        .order(ByteOrder.nativeOrder());
        promote(chunkArray, dataType, promoted);
        promoted.rewind();

        try {
            return ByteBuffer.wrap(
                    Zfp.compress(promoted.array(), zfpType, zfpShape, params(zfpType, zfpShape.length)));
        } catch (RuntimeException ex) {
            throw new ZarrException("Error in encoding zfp.", ex);
        }
    }

    @Override
    public Array decode(ByteBuffer chunkBytes) throws ZarrException {
        final DataType dataType = arrayMetadata.dataType.getMA2DataType();
        final Zfp.Type zfpType = zfpType(dataType);
        final int[] zfpShape = zfpShape();

        final byte[] promoted;
        try {
            promoted = Zfp.decompress(Utils.toArray(chunkBytes), zfpType, zfpShape,
                    params(zfpType, zfpShape.length));
        } catch (RuntimeException ex) {
            throw new ZarrException("Error in decoding zfp.", ex);
        }
        return demote(ByteBuffer.wrap(promoted).order(ByteOrder.nativeOrder()), dataType,
                (int) Zfp.countValues(zfpShape));
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Zfp codec.");
    }

    /**
     * The zfp scalar type the chunk's values are compressed as. Data types narrower than 32 bits are
     * compressed as {@code int32}, unsigned types as their signed counterpart.
     */
    private Zfp.Type zfpType(DataType dataType) throws ZarrException {
        switch (dataType) {
            case BYTE:
            case UBYTE:
            case SHORT:
            case USHORT:
            case INT:
            case UINT:
                return Zfp.Type.INT32;
            case LONG:
            case ULONG:
                return Zfp.Type.INT64;
            case FLOAT:
                return Zfp.Type.FLOAT;
            case DOUBLE:
                return Zfp.Type.DOUBLE;
            default:
                throw new ZarrException(
                        "The zfp codec does not support the data type '" + arrayMetadata.dataType + "'.");
        }
    }

    /**
     * The chunk shape as a zfp field shape, in row-major order. The chunk of a zero-dimensional array
     * is a 1D field holding a single value.
     */
    private int[] zfpShape() throws ZarrException {
        final int[] chunkShape = arrayMetadata.chunkShape;
        if (chunkShape.length == 0) {
            return new int[]{1};
        }
        if (chunkShape.length > Zfp.MAX_DIMS) {
            throw new ZarrException(
                    "The zfp codec supports at most " + Zfp.MAX_DIMS + " dimensions, but the chunk shape "
                            + Arrays.toString(chunkShape) + " has " + chunkShape.length + ".");
        }
        return chunkShape;
    }

    /**
     * Resolves the configured compression mode to zfp's expert mode parameters. Fixed-rate mode
     * depends on the zfp type and the dimensionality, which is why this cannot happen while parsing
     * the configuration.
     */
    private Zfp.Params params(Zfp.Type zfpType, int dims) throws ZarrException {
        try {
            switch (configuration.mode) {
                case FIXED_RATE:
                    return Zfp.Params.fixedRate(configuration.rate, zfpType, dims);
                default:
                    return configuration.resolveParams();
            }
        } catch (IllegalArgumentException ex) {
            throw new ZarrException("Invalid zfp codec configuration.", ex);
        }
    }

    /**
     * Writes the chunk's values into {@code out} as zfp scalars, promoting narrower integers into
     * {@code int32} as specified.
     */
    private void promote(Array chunkArray, DataType dataType, ByteBuffer out) {
        switch (dataType) {
            case BYTE: {
                final byte[] values = (byte[]) chunkArray.copyTo1DJavaArray();
                for (byte value : values) {
                    out.putInt(value << 23);
                }
                break;
            }
            case UBYTE: {
                final byte[] values = (byte[]) chunkArray.copyTo1DJavaArray();
                for (byte value : values) {
                    out.putInt(((value & 0xFF) - 0x80) << 23);
                }
                break;
            }
            case SHORT: {
                final short[] values = (short[]) chunkArray.copyTo1DJavaArray();
                for (short value : values) {
                    out.putInt(value << 15);
                }
                break;
            }
            case USHORT: {
                final short[] values = (short[]) chunkArray.copyTo1DJavaArray();
                for (short value : values) {
                    out.putInt(((value & 0xFFFF) - 0x8000) << 15);
                }
                break;
            }
            case INT: {
                final int[] values = (int[]) chunkArray.copyTo1DJavaArray();
                for (int value : values) {
                    out.putInt(value);
                }
                break;
            }
            case UINT: {
                final int[] values = (int[]) chunkArray.copyTo1DJavaArray();
                for (int value : values) {
                    // Values above Integer.MAX_VALUE have their sign bit set and are clamped
                    out.putInt(value < 0 ? Integer.MAX_VALUE : value);
                }
                break;
            }
            case LONG: {
                final long[] values = (long[]) chunkArray.copyTo1DJavaArray();
                for (long value : values) {
                    out.putLong(value);
                }
                break;
            }
            case ULONG: {
                final long[] values = (long[]) chunkArray.copyTo1DJavaArray();
                for (long value : values) {
                    // Values above Long.MAX_VALUE have their sign bit set and are clamped
                    out.putLong(value < 0 ? Long.MAX_VALUE : value);
                }
                break;
            }
            case FLOAT: {
                final float[] values = (float[]) chunkArray.copyTo1DJavaArray();
                for (float value : values) {
                    out.putFloat(value);
                }
                break;
            }
            case DOUBLE: {
                final double[] values = (double[]) chunkArray.copyTo1DJavaArray();
                for (double value : values) {
                    out.putDouble(value);
                }
                break;
            }
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    /**
     * Reads {@code valueCount} zfp scalars from {@code in} and demotes them back into the chunk's data
     * type.
     */
    private Array demote(ByteBuffer in, DataType dataType, int valueCount) {
        final int[] shape = arrayMetadata.chunkShape;
        switch (dataType) {
            case BYTE:
            case UBYTE: {
                final byte[] values = new byte[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    final int value = in.getInt() >> 23;
                    values[i] = dataType == DataType.BYTE
                            ? (byte) clamp(value, -0x80, 0x7F)
                            : (byte) clamp(value + 0x80, 0x00, 0xFF);
                }
                return Array.factory(dataType, shape, values);
            }
            case SHORT:
            case USHORT: {
                final short[] values = new short[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    final int value = in.getInt() >> 15;
                    values[i] = dataType == DataType.SHORT
                            ? (short) clamp(value, -0x8000, 0x7FFF)
                            : (short) clamp(value + 0x8000, 0x0000, 0xFFFF);
                }
                return Array.factory(dataType, shape, values);
            }
            case INT:
            case UINT: {
                final int[] values = new int[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    final int value = in.getInt();
                    values[i] = dataType == DataType.INT ? value : Math.max(value, 0);
                }
                return Array.factory(dataType, shape, values);
            }
            case LONG:
            case ULONG: {
                final long[] values = new long[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    final long value = in.getLong();
                    values[i] = dataType == DataType.LONG ? value : Math.max(value, 0);
                }
                return Array.factory(dataType, shape, values);
            }
            case FLOAT: {
                final float[] values = new float[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    values[i] = in.getFloat();
                }
                return Array.factory(dataType, shape, values);
            }
            case DOUBLE: {
                final double[] values = new double[valueCount];
                for (int i = 0; i < valueCount; i++) {
                    values[i] = in.getDouble();
                }
                return Array.factory(dataType, shape, values);
            }
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    /**
     * The zfp <a href="https://zfp.readthedocs.io/en/latest/modes.html">compression modes</a>.
     */
    public enum Mode {
        REVERSIBLE("reversible"),
        EXPERT("expert"),
        FIXED_ACCURACY("fixed_accuracy"),
        FIXED_RATE("fixed_rate"),
        FIXED_PRECISION("fixed_precision");

        private final String mode;

        Mode(String mode) {
            this.mode = mode;
        }

        @JsonCreator
        public static Mode fromValue(String value) {
            for (Mode mode : values()) {
                if (mode.mode.equals(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown zfp mode '" + value + "'.");
        }

        @JsonValue
        public String getValue() {
            return mode;
        }
    }

    public static final class Configuration {

        @Nonnull
        public final Mode mode;
        @Nullable
        public final Integer minbits;
        @Nullable
        public final Integer maxbits;
        @Nullable
        public final Integer maxprec;
        @Nullable
        public final Integer minexp;
        @Nullable
        public final Double tolerance;
        @Nullable
        public final Double rate;
        @Nullable
        public final Integer precision;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(
                @Nonnull @JsonProperty(value = "mode", required = true) Mode mode,
                @Nullable @JsonProperty("minbits") Integer minbits,
                @Nullable @JsonProperty("maxbits") Integer maxbits,
                @Nullable @JsonProperty("maxprec") Integer maxprec,
                @Nullable @JsonProperty("minexp") Integer minexp,
                @Nullable @JsonProperty("tolerance") Double tolerance,
                @Nullable @JsonProperty("rate") Double rate,
                @Nullable @JsonProperty("precision") Integer precision
        ) throws ZarrException {
            if (mode == null) {
                throw new ZarrException("'mode' is required for the zfp codec.");
            }
            this.mode = mode;
            this.minbits = minbits;
            this.maxbits = maxbits;
            this.maxprec = maxprec;
            this.minexp = minexp;
            this.tolerance = tolerance;
            this.rate = rate;
            this.precision = precision;

            switch (mode) {
                case REVERSIBLE:
                    requireAbsent("minbits", minbits, "maxbits", maxbits, "maxprec", maxprec, "minexp",
                            minexp, "tolerance", tolerance, "rate", rate, "precision", precision);
                    break;
                case EXPERT:
                    requirePresent("minbits", minbits, "maxbits", maxbits, "maxprec", maxprec, "minexp",
                            minexp);
                    requireAbsent("tolerance", tolerance, "rate", rate, "precision", precision);
                    break;
                case FIXED_ACCURACY:
                    requirePresent("tolerance", tolerance);
                    requireAbsent("minbits", minbits, "maxbits", maxbits, "maxprec", maxprec, "minexp",
                            minexp, "rate", rate, "precision", precision);
                    break;
                case FIXED_RATE:
                    requirePresent("rate", rate);
                    requireAbsent("minbits", minbits, "maxbits", maxbits, "maxprec", maxprec, "minexp",
                            minexp, "tolerance", tolerance, "precision", precision);
                    if (!(rate > 0)) {
                        throw new ZarrException("'rate' needs to be positive, got " + rate + ".");
                    }
                    break;
                case FIXED_PRECISION:
                    requirePresent("precision", precision);
                    requireAbsent("minbits", minbits, "maxbits", maxbits, "maxprec", maxprec, "minexp",
                            minexp, "tolerance", tolerance, "rate", rate);
                    break;
            }
            if (mode != Mode.FIXED_RATE) {
                // Fails fast on out-of-range parameters. Fixed-rate mode needs the data type and the
                // dimensionality, so it can only be resolved once the array metadata is known.
                try {
                    resolveParams();
                } catch (IllegalArgumentException ex) {
                    throw new ZarrException("Invalid zfp codec configuration.", ex);
                }
            }
        }

        public static Configuration reversible() throws ZarrException {
            return new Configuration(Mode.REVERSIBLE, null, null, null, null, null, null, null);
        }

        public static Configuration expert(int minbits, int maxbits, int maxprec, int minexp)
                throws ZarrException {
            return new Configuration(Mode.EXPERT, minbits, maxbits, maxprec, minexp, null, null, null);
        }

        public static Configuration fixedAccuracy(double tolerance) throws ZarrException {
            return new Configuration(Mode.FIXED_ACCURACY, null, null, null, null, tolerance, null, null);
        }

        public static Configuration fixedRate(double rate) throws ZarrException {
            return new Configuration(Mode.FIXED_RATE, null, null, null, null, null, rate, null);
        }

        public static Configuration fixedPrecision(int precision) throws ZarrException {
            return new Configuration(Mode.FIXED_PRECISION, null, null, null, null, null, null, precision);
        }

        private static void requirePresent(Object... namesAndValues) throws ZarrException {
            for (int i = 0; i < namesAndValues.length; i += 2) {
                if (namesAndValues[i + 1] == null) {
                    throw new ZarrException(
                            "'" + namesAndValues[i] + "' is required for the zfp codec.");
                }
            }
        }

        private static void requireAbsent(Object... namesAndValues) throws ZarrException {
            for (int i = 0; i < namesAndValues.length; i += 2) {
                if (namesAndValues[i + 1] != null) {
                    throw new ZarrException(
                            "'" + namesAndValues[i] + "' is not a parameter of this zfp mode.");
                }
            }
        }

        /**
         * Resolves this configuration to zfp's expert mode parameters. Not supported for fixed-rate
         * mode, which additionally depends on the data type and the dimensionality of the chunk.
         */
        @JsonIgnore
        Zfp.Params resolveParams() {
            switch (mode) {
                case REVERSIBLE:
                    return Zfp.Params.reversible();
                case EXPERT:
                    return Zfp.Params.expert(minbits, maxbits, maxprec, minexp);
                case FIXED_ACCURACY:
                    return Zfp.Params.fixedAccuracy(tolerance);
                case FIXED_PRECISION:
                    return Zfp.Params.fixedPrecision(precision);
                default:
                    throw new IllegalStateException(
                            "The zfp parameters of mode '" + mode.getValue() + "' depend on the array metadata.");
            }
        }
    }
}
