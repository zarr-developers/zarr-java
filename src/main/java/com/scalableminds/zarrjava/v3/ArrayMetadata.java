package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.v3.chunkgrid.ChunkGrid;
import com.scalableminds.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import com.scalableminds.zarrjava.v3.codec.Codec;
import com.scalableminds.zarrjava.v3.chunkgrid.RegularChunkGrid;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ArrayMetadata {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "array";

    public Map<String, Object> attributes;

    public long[] shape;

    @JsonProperty("data_type")
    public DataType dataType;

    @JsonProperty("chunk_grid")
    public ChunkGrid chunkGrid;

    @JsonProperty("chunk_key_encoding")
    public ChunkKeyEncoding chunkKeyEncoding;

    @JsonProperty("fill_value")
    public Object fillValue;

    public Optional<Codec[]> codecs;

    @JsonProperty("dimension_names")
    public Optional<String[]> dimensionNames;

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
                    ;
                    for (int i = 0; i < dataType.getByteCount(); i++) {
                        b.put((byte) Integer.parseInt(fillValueString.substring(2 + i * 2, 2 + (i + 1) * 2), 16));
                    }
                    return b;
                });
            }
        }
        throw new RuntimeException(String.format("Invalid fillValue %s", fillValue));
    }

    public int ndim() {
        return shape.length;
    }

    public int[] chunkShape() {
        return ((RegularChunkGrid) this.chunkGrid).configuration.chunkShape;
    }

    public int chunkSize() {
        return Arrays.stream(((RegularChunkGrid) this.chunkGrid).configuration.chunkShape).reduce(1,
                (acc, a) -> acc * a);
    }

    public int chunkByteLength() {
        return this.dataType.getByteCount() * chunkSize();
    }
}
