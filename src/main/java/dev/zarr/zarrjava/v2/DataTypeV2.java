package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.DataType;

public enum DataTypeV2 implements dev.zarr.zarrjava.interfaces.DataType{
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
  FLOAT64("f8", Endianness.LITTLE);

  private final String dtype;
  private final Endianness endianness;

  DataTypeV2(String dtype, Endianness endianness) {
    this.dtype = dtype;
    this.endianness = endianness;
  }

  public static DataTypeV2 fromDataType(DataType dataType) {
    switch (dataType) {
      case BOOL:
        return BOOL;
      case INT8:
        return INT8;
      case INT16:
        return INT16;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case UINT8:
        return UINT8;
      case UINT16:
        return UINT16;
      case UINT32:
        return UINT32;
      case UINT64:
        return UINT64;
      case FLOAT32:
        return FLOAT32;
      case FLOAT64:
        return FLOAT64;
      default:
        throw new IllegalArgumentException("Unsupported DataType: " + dataType);
    }
  }

  public Endianness getEndianness() {
    return endianness;
  }

  public DataType toV3() throws ZarrException {
    if (this.dtype.equals(BOOL.dtype))
      return DataType.BOOL;
    if (this.dtype.equals(INT8.dtype))
      return DataType.INT8;
    if (this.dtype.equals(INT16.dtype))
      return DataType.INT16;
    if (this.dtype.equals(INT32.dtype))
      return DataType.INT32;
    if (this.dtype.equals(INT64.dtype))
      return DataType.INT64;
    if (this.dtype.equals(UINT8.dtype))
      return DataType.UINT8;
    if (this.dtype.equals(UINT16.dtype))
      return DataType.UINT16;
    if (this.dtype.equals(UINT32.dtype))
      return DataType.UINT32;
    if (this.dtype.equals(UINT64.dtype))
      return DataType.UINT64;
    if (this.dtype.equals(FLOAT32.dtype))
      return DataType.FLOAT32;
    if (this.dtype.equals(FLOAT64.dtype))
      return DataType.FLOAT64;
    throw new ZarrException("Unknown DataTypeV2: " + this.dtype);
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
        return ucar.ma2.DataType.SHORT;
      case INT32:
        return ucar.ma2.DataType.INT;
      case INT64:
        return ucar.ma2.DataType.LONG;
      case UINT8:
        return ucar.ma2.DataType.UBYTE;
      case UINT16:
        return ucar.ma2.DataType.USHORT;
      case UINT32:
        return ucar.ma2.DataType.UINT;
      case UINT64:
        return ucar.ma2.DataType.ULONG;
      case FLOAT32:
        return ucar.ma2.DataType.FLOAT;
      case FLOAT64:
        return ucar.ma2.DataType.DOUBLE;
      default:
        throw new RuntimeException("Unreachable");
    }
  }

}
