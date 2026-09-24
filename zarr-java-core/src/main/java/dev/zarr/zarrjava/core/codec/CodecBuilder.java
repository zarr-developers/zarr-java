package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.core.DataType;

public abstract class CodecBuilder {

    final protected DataType dataType;

    public CodecBuilder(DataType dataType) {
        this.dataType = dataType;
    }

    public abstract Codec[] build();
}
