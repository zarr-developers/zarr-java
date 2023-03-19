package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {
    BOOL("bool"),
    INT8("int8"),
    INT16("int16"),
    INT32("int32"),
    INT64("int64"),
    UINT8("uint8"),
    UINT16("uint16"),
    UINT32("uint32"),
    UINT64("uint64"),
    FLOAT32("float32"),
    FLOAT64("float64");

    private String dtype;

    private DataType(String dtype) {
        this.dtype = dtype;
    }

    @JsonValue
    public String getValue() {
        return dtype;
    }
}
