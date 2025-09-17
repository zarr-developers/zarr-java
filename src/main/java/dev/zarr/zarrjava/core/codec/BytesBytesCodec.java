package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface BytesBytesCodec {

    ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException;

    ByteBuffer decode(ByteBuffer chunkBytes) throws ZarrException;

    default void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[4096];
        int len;
        while ((len = inputStream.read(buffer)) > 0) {
          outputStream.write(buffer, 0, len);
        }
      }
}
