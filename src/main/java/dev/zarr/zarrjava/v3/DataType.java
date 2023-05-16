package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {
    BOOL("bool", 1),
    INT8("int8", 1),
    INT16("int16", 2),
    INT32("int32", 4),
    INT64("int64", 8),
    UINT8("uint8", 1),
    UINT16("uint16", 2),
    UINT32("uint32", 4),
    UINT64("uint64", 8),
    FLOAT32("float32", 4),
    FLOAT64("float64", 8);

    private final String dtype;
    private final int byteCount;

    DataType(String dtype, int byteCount) {
        this.dtype = dtype;
        this.byteCount = byteCount;
    }

    @JsonValue
    public String getValue() {
        return dtype;
    }

    public int getByteCount() {return byteCount;}
}
