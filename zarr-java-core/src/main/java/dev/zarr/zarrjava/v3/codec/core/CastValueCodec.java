package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata.CoreArrayMetadata;
import dev.zarr.zarrjava.core.codec.ArrayArrayCodec;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.core.CastValueConverter.ScalarEntry;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code cast_value} codec converts (casts) the numeric value of every array element to a
 * different data type. It is an {@code array -> array} codec: it changes the stored data type while
 * leaving every other array property intact, and it does not reinterpret binary representations.
 *
 * <p>On encode the values are cast from the array data type to {@code configuration.data_type}; on
 * decode the same procedure runs with the input and output data types swapped. The actual numeric
 * conversion lives in {@link CastValueConverter}; this class only handles the Zarr codec integration:
 * configuration parsing, propagating the new data type to downstream codecs, and casting the fill
 * value.
 *
 * <p>Supported data types are the real-number types this library models: {@code int8/16/32/64},
 * {@code uint8/16/32/64}, {@code float32} and {@code float64}. Other data types from the codec
 * specification (e.g. {@code float8_*}, {@code bfloat16}, {@code int2}) are not modelled here.
 */
public class CastValueCodec extends ArrayArrayCodec implements Codec {

    @JsonIgnore
    @Nonnull
    public final String name = "cast_value";
    @Nonnull
    public final Configuration configuration;

    // Set up in resolveArrayMetadata (once, before any encode/decode call), then only read.
    @JsonIgnore
    private CastValueConverter converter;
    @JsonIgnore
    private List<ScalarEntry> encodeEntries = new ArrayList<>();
    @JsonIgnore
    private List<ScalarEntry> decodeEntries = new ArrayList<>();

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CastValueCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) {
        this.configuration = configuration;
    }

    // ===== Codec pipeline integration ========================================================

    @Override
    public Array encode(Array chunkArray) throws ZarrException {
        return converter.castArray(chunkArray, arrayDataType(), configuration.dataType, encodeEntries);
    }

    @Override
    public Array decode(Array chunkArray) throws ZarrException {
        return converter.castArray(chunkArray, configuration.dataType, arrayDataType(), decodeEntries);
    }

    @Override
    public long computeEncodedSize(long inputByteLength, CoreArrayMetadata arrayMetadata)
            throws ZarrException {
        long numElements = inputByteLength / arrayMetadata.dataType.getByteCount();
        return numElements * configuration.dataType.getByteCount();
    }

    /**
     * Runs once when the codec pipeline is built. It validates the configuration, prepares the
     * converter and scalar_map lookups, casts the fill value to the target type, and reports the new
     * data type (and cast fill value) to the codecs downstream of this one.
     */
    @Override
    public CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        super.resolveArrayMetadata();
        DataType inputType = arrayDataType();
        DataType outputType = configuration.dataType;
        CastValueConverter.requireSupported(inputType);
        CastValueConverter.requireSupported(outputType);
        if (configuration.outOfRange == OutOfRange.WRAP && CastValueConverter.isFloatTarget(outputType)) {
            throw new ZarrException(
                    "The cast_value 'out_of_range' value 'wrap' is only permitted for integral target data types.");
        }

        this.converter = new CastValueConverter(configuration.rounding, configuration.outOfRange);
        this.encodeEntries = converter.buildEntries(
                configuration.scalarMap == null ? null : configuration.scalarMap.encode, inputType, outputType);
        this.decodeEntries = converter.buildEntries(
                configuration.scalarMap == null ? null : configuration.scalarMap.decode, outputType, inputType);

        Object srcFillValue = arrayMetadata.parsedFillValue;
        Object castFillValue = converter.castFillValue(srcFillValue, inputType, outputType, encodeEntries);
        if (srcFillValue != null
                && !converter.fillValueRoundTrips(castFillValue, srcFillValue, outputType, inputType, decodeEntries)) {
            throw new ZarrException(
                    "The cast_value fill value '" + srcFillValue + "' does not survive a round-trip cast.");
        }

        return new CoreArrayMetadata(
                arrayMetadata.shape, arrayMetadata.chunkShape, outputType, castFillValue);
    }

    private DataType arrayDataType() throws ZarrException {
        if (!(arrayMetadata.dataType instanceof DataType)) {
            throw new ZarrException("The cast_value codec requires a Zarr v3 data type.");
        }
        return (DataType) arrayMetadata.dataType;
    }

    // ===== Configuration =====================================================================

    /** How values are rounded when the target data type cannot exactly represent a value. */
    public enum Rounding {
        NEAREST_EVEN("nearest-even", RoundingMode.HALF_EVEN),
        TOWARDS_ZERO("towards-zero", RoundingMode.DOWN),
        TOWARDS_POSITIVE("towards-positive", RoundingMode.CEILING),
        TOWARDS_NEGATIVE("towards-negative", RoundingMode.FLOOR),
        NEAREST_AWAY("nearest-away", RoundingMode.HALF_UP);

        private final String value;
        final RoundingMode mode;

        Rounding(String value, RoundingMode mode) {
            this.value = value;
            this.mode = mode;
        }

        @JsonValue
        public String getValue() {
            return value;
        }

        @JsonCreator
        public static Rounding fromValue(String value) {
            for (Rounding rounding : values()) {
                if (rounding.value.equals(value)) {
                    return rounding;
                }
            }
            throw new IllegalArgumentException("Unknown cast_value rounding: '" + value + "'.");
        }
    }

    /** How values outside the representable range of the target data type are handled. */
    public enum OutOfRange {
        CLAMP("clamp"),
        WRAP("wrap");

        private final String value;

        OutOfRange(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }

        @JsonCreator
        public static OutOfRange fromValue(String value) {
            for (OutOfRange outOfRange : values()) {
                if (outOfRange.value.equals(value)) {
                    return outOfRange;
                }
            }
            throw new IllegalArgumentException("Unknown cast_value out_of_range: '" + value + "'.");
        }
    }

    /** Explicit input-to-output scalar mappings, evaluated before any other casting rule. */
    public static final class ScalarMap {
        @Nullable
        @JsonProperty("encode")
        public final Object[][] encode;
        @Nullable
        @JsonProperty("decode")
        public final Object[][] decode;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ScalarMap(
                @Nullable @JsonProperty("encode") Object[][] encode,
                @Nullable @JsonProperty("decode") Object[][] decode) {
            this.encode = encode;
            this.decode = decode;
        }
    }

    public static final class Configuration {

        @Nonnull
        @JsonProperty("data_type")
        public final DataType dataType;

        @Nonnull
        @JsonProperty("rounding")
        public final Rounding rounding;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("out_of_range")
        public final OutOfRange outOfRange;

        @Nullable
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("scalar_map")
        public final ScalarMap scalarMap;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(
                @Nonnull @JsonProperty(value = "data_type", required = true) DataType dataType,
                @Nullable @JsonProperty("rounding") Rounding rounding,
                @Nullable @JsonProperty("out_of_range") OutOfRange outOfRange,
                @Nullable @JsonProperty("scalar_map") ScalarMap scalarMap) {
            this.dataType = dataType;
            this.rounding = rounding == null ? Rounding.NEAREST_EVEN : rounding;
            this.outOfRange = outOfRange;
            this.scalarMap = scalarMap;
        }
    }
}
