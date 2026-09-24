package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;

import java.nio.ByteBuffer;

public abstract class BytesBytesCodec extends AbstractCodec {

    public abstract ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException;

    public abstract ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException;

}
