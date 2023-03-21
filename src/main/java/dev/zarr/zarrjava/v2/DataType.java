package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {
    BOOL("b1", Endianness.UNSPECIFIED), INT8("i1", Endianness.UNSPECIFIED), INT16("i2", Endianness.LITTLE), INT32("i4",
            Endianness.LITTLE), INT64("i8", Endianness.LITTLE), UINT8("u1", Endianness.UNSPECIFIED), UINT16("u2",
            Endianness.LITTLE), UINT32("u4", Endianness.LITTLE), UINT64("u8", Endianness.LITTLE), FLOAT32("f4",
            Endianness.LITTLE), FLOAT64("f8", Endianness.LITTLE);

    private final String dtype;
    private final Endianness endianness;

    DataType(String dtype, Endianness endianness) {
        this.dtype = dtype;
        this.endianness = endianness;
    }

    @JsonValue
    public String getValue() {
        return String.format("%s%s", endianness.getValue(), dtype);
    }
}
