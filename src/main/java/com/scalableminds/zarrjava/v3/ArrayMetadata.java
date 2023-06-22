package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.v3.chunkgrid.ChunkGrid;
import com.scalableminds.zarrjava.v3.chunkgrid.RegularChunkGrid;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import com.scalableminds.zarrjava.v3.codec.Codec;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;


public final class ArrayMetadata {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "array";


    public final long[] shape;

    @JsonProperty("data_type")
    public final DataType dataType;

    @JsonProperty("chunk_grid")
    public final ChunkGrid chunkGrid;

    @JsonProperty("chunk_key_encoding")
    public final ChunkKeyEncoding chunkKeyEncoding;

    @JsonProperty("fill_value")
    public final Object fillValue;

    @Nullable
    @JsonProperty("codecs")
    public final Codec[] codecs;
    @Nullable
    @JsonProperty("attributes")
    public final Map<String, Object> attributes;
    @Nullable
    @JsonProperty("dimension_names")
    public String[] dimensionNames;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ArrayMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @JsonProperty(value = "shape", required = true) long[] shape,
            @JsonProperty(value = "data_type", required = true) DataType dataType,
            @JsonProperty(value = "chunk_grid", required = true) ChunkGrid chunkGrid,
            @JsonProperty(value = "chunk_key_encoding", required = true) ChunkKeyEncoding chunkKeyEncoding,
            @JsonProperty(value = "fill_value", required = true) Object fillValue,
            @Nullable @JsonProperty(value = "codecs") Codec[] codecs,
            @Nullable @JsonProperty(value = "dimension_names") String[] dimensionNames,
            @Nullable @JsonProperty(value = "attributes") Map<String, Object> attributes) {
        assert zarrFormat == 3;
        assert nodeType.equals("array");
        this.shape = shape;
        this.dataType = dataType;
        this.chunkGrid = chunkGrid;
        this.chunkKeyEncoding = chunkKeyEncoding;
        this.fillValue = fillValue;
        this.codecs = codecs;
        this.dimensionNames = dimensionNames;
        this.attributes = attributes;
    }

    public static ByteBuffer getFillValueBytes(Object fillValue, DataType dataType) {
        if (fillValue instanceof Boolean) {
            Boolean fillValueBool = (Boolean) fillValue;
            if (Objects.requireNonNull(dataType) == DataType.BOOL) {
                return Utils.makeByteBuffer(1, b -> b.put((byte) (fillValueBool ? 1 : 0)));
            }
        }
        if (fillValue instanceof Number) {
            Number fillValueNumber = (Number) fillValue;
            switch (dataType) {
                case BOOL:
                case INT8:
                case UINT8:
                    return Utils.makeByteBuffer(1, b -> b.put(fillValueNumber.byteValue()));
                case INT16:
                case UINT16:
                    return Utils.makeByteBuffer(2, b -> b.putShort(fillValueNumber.shortValue()));
                case INT32:
                case UINT32:
                    return Utils.makeByteBuffer(4, b -> b.putInt(fillValueNumber.shortValue()));
                case INT64:
                case UINT64:
                    return Utils.makeByteBuffer(8, b -> b.putLong(fillValueNumber.shortValue()));
                case FLOAT32:
                    return Utils.makeByteBuffer(4, b -> b.putFloat(fillValueNumber.floatValue()));
                case FLOAT64:
                    return Utils.makeByteBuffer(8, b -> b.putDouble(fillValueNumber.doubleValue()));
                default:
                    // Fallback to throwing below
            }
        } else if (fillValue instanceof String) {
            String fillValueString = (String) fillValue;
            if (fillValueString.equals("NaN")) {
                switch (dataType) {
                    case FLOAT32:
                        return Utils.makeByteBuffer(4, b -> b.putFloat(Float.NaN));
                    case FLOAT64:
                        return Utils.makeByteBuffer(8, b -> b.putDouble(Double.NaN));
                    default:
                        throw new RuntimeException(
                                String.format("Invalid fillValue %s for dataType %s.", fillValueString, dataType));
                }
            } else if (fillValueString.equals("+Infinity")) {
                switch (dataType) {
                    case FLOAT32:
                        return Utils.makeByteBuffer(4, b -> b.putFloat(Float.POSITIVE_INFINITY));
                    case FLOAT64:
                        return Utils.makeByteBuffer(8, b -> b.putDouble(Double.POSITIVE_INFINITY));
                    default:
                        throw new RuntimeException(
                                String.format("Invalid fillValue %s for dataType %s.", fillValueString, dataType));
                }
            } else if (fillValueString.equals("-Infinity")) {
                switch (dataType) {
                    case FLOAT32:
                        return Utils.makeByteBuffer(4, b -> b.putFloat(Float.NEGATIVE_INFINITY));
                    case FLOAT64:
                        return Utils.makeByteBuffer(8, b -> b.putDouble(Double.NEGATIVE_INFINITY));
                    default:
                        throw new RuntimeException(
                                String.format("Invalid fillValue %s for dataType %s.", fillValueString, dataType));
                }
            } else if (fillValueString.startsWith("0b")) {
                return Utils.makeByteBuffer(dataType.getByteCount(), b -> {
                    for (int i = 0; i < dataType.getByteCount(); i++) {
                        b.put((byte) Integer.parseInt(fillValueString.substring(2 + i * 8, 2 + (i + 1) * 8), 2));
                    }
                    return b;
                });
            } else if (fillValueString.startsWith("0x")) {
                return Utils.makeByteBuffer(dataType.getByteCount(), b -> {
                    for (int i = 0; i < dataType.getByteCount(); i++) {
                        b.put((byte) Integer.parseInt(fillValueString.substring(2 + i * 2, 2 + (i + 1) * 2), 16));
                    }
                    return b;
                });
            }
        }
        throw new RuntimeException(String.format("Invalid fillValue %s", fillValue));
    }

    public CoreArrayMetadata getCoreMetadata() {
        return new CoreArrayMetadata(shape, chunkShape(), dataType, fillValue);
    }

    public ucar.ma2.Array allocateFillValueChunk() {
        return getCoreMetadata().allocateFillValueChunk();
    }

    public int ndim() {
        return shape.length;
    }

    public int[] chunkShape() {
        return ((RegularChunkGrid) this.chunkGrid).configuration.chunkShape;
    }

    public int chunkSize() {
        return getCoreMetadata().chunkSize();
    }

    public int chunkByteLength() {
        return getCoreMetadata().chunkByteLength();
    }

    public static final class CoreArrayMetadata {
        public final long[] shape;
        public final int[] chunkShape;
        public final DataType dataType;
        public final Object fillValue;

        public CoreArrayMetadata(long[] shape, int[] chunkShape, DataType dataType, Object fillValue) {
            this.shape = shape;
            this.chunkShape = chunkShape;
            this.dataType = dataType;
            this.fillValue = fillValue;
        }

        public int ndim() {
            return shape.length;
        }

        public int chunkSize() {
            return Arrays.stream(chunkShape).reduce(1, (acc, a) -> acc * a);
        }

        public int chunkByteLength() {
            return this.dataType.getByteCount() * chunkSize();
        }

        public ucar.ma2.Array allocateFillValueChunk() {
            int byteLength = chunkByteLength();
            ByteBuffer fillValueBytes = ArrayMetadata.getFillValueBytes(fillValue, dataType);

            return ucar.ma2.Array.factory(dataType.getMA2DataType(), chunkShape, Utils.makeByteBuffer(byteLength, b -> {
                for (int i = 0; i < chunkSize(); i++) {
                    b.put(fillValueBytes);
                }
                return b;
            }));
        }
    }
}
