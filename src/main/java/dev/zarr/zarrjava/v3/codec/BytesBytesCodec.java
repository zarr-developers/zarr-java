package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;

import java.nio.ByteBuffer;

public abstract class BytesBytesCodec extends Codec {

    protected abstract ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException;

    public abstract ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException;

}
