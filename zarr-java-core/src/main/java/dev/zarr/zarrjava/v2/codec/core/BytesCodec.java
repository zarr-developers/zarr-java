package dev.zarr.zarrjava.v2.codec.core;

import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.v2.codec.Codec;

import javax.annotation.Nonnull;
import java.nio.ByteOrder;

public class BytesCodec extends dev.zarr.zarrjava.core.codec.core.BytesCodec implements Codec {
    public final String name = "bytes";
    @Nonnull
    public final Endian endian;

    public BytesCodec(Endian endian) {
        this.endian = endian;
    }

    @Override
    protected ByteOrder getByteOrder() {
        return endian.getByteOrder();
    }

    @Override
    public Codec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return this;
    }
}

