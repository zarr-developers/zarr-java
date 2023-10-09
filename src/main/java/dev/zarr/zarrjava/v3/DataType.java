package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonValue;

public enum DataType {
  BOOL("bool", 1),
  INT8("int8", 1),
  INT16("int16", 2),
  INT32("int32", 4),
  INT64("int64", 8),
  UINT8(
      "uint8",
      1
  ),
  UINT16("uint16", 2),
  UINT32("uint32", 4),
  UINT64("uint64", 8),
  FLOAT32("float32", 4),
  FLOAT64(
      "float64",
      8
  );

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

  public int getByteCount() {
    return byteCount;
  }

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
