package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType implements dev.zarr.zarrjava.core.DataType {
    BOOL("b1", Endianness.UNSPECIFIED),
    INT8("i1", Endianness.UNSPECIFIED),
    INT16("i2", Endianness.LITTLE),
    INT32("i4", Endianness.LITTLE),
    INT64("i8", Endianness.LITTLE),
    UINT8("u1", Endianness.UNSPECIFIED),
    UINT16("u2", Endianness.LITTLE),
    UINT32("u4", Endianness.LITTLE),
    UINT64("u8", Endianness.LITTLE),
    FLOAT32("f4", Endianness.LITTLE),
    FLOAT64("f8", Endianness.LITTLE),
    INT16_BE("i2", Endianness.BIG),
    INT32_BE("i4", Endianness.BIG),
    INT64_BE("i8", Endianness.BIG),
    UINT16_BE("u2", Endianness.BIG),
    UINT32_BE("u4", Endianness.BIG),
    UINT64_BE("u8", Endianness.BIG),
    FLOAT32_BE("f4", Endianness.BIG),
    FLOAT64_BE("f8", Endianness.BIG);

    private final String dtype;
    private final Endianness endianness;


    DataType(String dtype, Endianness endianness) {
        this.dtype = dtype;
        this.endianness = endianness;
    }

    public Endianness getEndianness() {
        return endianness;
    }

    @JsonValue
    public String getValue() {
        return String.format("%s%s", endianness.getValue(), dtype);
    }

    @Override
    public ucar.ma2.DataType getMA2DataType() {
        switch (this) {
            case BOOL:
                return ucar.ma2.DataType.BOOLEAN;
            case INT8:
                return ucar.ma2.DataType.BYTE;
            case INT16:
            case INT16_BE:
                return ucar.ma2.DataType.SHORT;
            case INT32:
            case INT32_BE:
                return ucar.ma2.DataType.INT;
            case INT64:
            case INT64_BE:
                return ucar.ma2.DataType.LONG;
            case UINT8:
                return ucar.ma2.DataType.UBYTE;
            case UINT16:
            case UINT16_BE:
                return ucar.ma2.DataType.USHORT;
            case UINT32:
            case UINT32_BE:
                return ucar.ma2.DataType.UINT;
            case UINT64:
            case UINT64_BE:
                return ucar.ma2.DataType.ULONG;
            case FLOAT32:
            case FLOAT32_BE:
                return ucar.ma2.DataType.FLOAT;
            case FLOAT64:
            case FLOAT64_BE:
                return ucar.ma2.DataType.DOUBLE;
            default:
                throw new RuntimeException("Unreachable");
        }
    }

    @Override
    public int getByteCount() {
        return Integer.parseInt(dtype.substring(1));
    }

}
