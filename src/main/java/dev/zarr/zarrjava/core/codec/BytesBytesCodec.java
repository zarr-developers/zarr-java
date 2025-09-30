package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class BytesBytesCodec extends AbstractCodec {

    public abstract ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException;

    public abstract ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException;

}
