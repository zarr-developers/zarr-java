package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.v2.codec.core.BytesCodec;

public enum Endianness {
  LITTLE("<"),
  BIG(">"),
  UNSPECIFIED("|");

  private final String value;

  Endianness(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }

  public BytesCodec.Endian toEndian() {
    switch (this) {
      case LITTLE:
        return BytesCodec.Endian.LITTLE;
      case BIG:
        return BytesCodec.Endian.BIG;
      case UNSPECIFIED:
      default:
        return BytesCodec.Endian.LITTLE;
    }
  }
}