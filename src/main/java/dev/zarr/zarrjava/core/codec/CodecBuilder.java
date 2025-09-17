package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.v3.DataType;

public abstract class CodecBuilder {

  final protected DataType dataType;

  public CodecBuilder(DataType dataType) {
    this.dataType = dataType;
  }

  public abstract Codec[] build();
}
