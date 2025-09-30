package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.utils.CRC32C;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.core.ArrayMetadata.CoreArrayMetadata;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Crc32cCodec extends  BytesBytesCodec implements Codec {

  public final String name = "crc32c";

  @JsonCreator
  public Crc32cCodec(){}

  @Override
  public ByteBuffer decode(ByteBuffer chunkBytes)
      throws ZarrException {
    ByteBuffer buffer = chunkBytes.slice();
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.limit(buffer.capacity() - 4);

    final CRC32C crc32c = new CRC32C();
    crc32c.update(buffer);
    int computedCrc32c = (int) crc32c.getValue();

    buffer.limit(buffer.capacity());
    int storedCrc32c = buffer.getInt();

    if (computedCrc32c != storedCrc32c) {
      throw new ZarrException(
          "The checksum of the sharding index is invalid. Stored: " + storedCrc32c + " "
              + "Computed: " +
              computedCrc32c);
    }
    buffer.rewind();
    buffer.limit(buffer.capacity() - 4);
    return buffer.slice();
  }

  @Override
  public ByteBuffer encode(ByteBuffer chunkBytes) {
    return Utils.makeByteBuffer(chunkBytes.capacity() + 4, b -> {
      final CRC32C crc32c = new CRC32C();
      crc32c.update(chunkBytes);
      int computedCrc32c = (int) crc32c.getValue();
      chunkBytes.rewind();
      b.put(chunkBytes);
      b.putInt(computedCrc32c);
      return b;
    });
  }

  @Override
  public long computeEncodedSize(long inputByteLength,
      CoreArrayMetadata arrayMetadata) throws ZarrException {
    return inputByteLength + 4;
  }
}

