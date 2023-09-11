package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.utils.CRC32C;
import com.scalableminds.zarrjava.utils.Utils;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.BytesBytesCodec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Crc32cCodec implements BytesBytesCodec {

  public final String name = "crc32c";

  @JsonCreator
  public Crc32cCodec(
  ) {
  }

  @Override
  public ByteBuffer decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata)
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
  public ByteBuffer encode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
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
}

