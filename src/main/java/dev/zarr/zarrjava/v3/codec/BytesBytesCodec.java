package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata.CoreArrayMetadata;
import java.nio.ByteBuffer;

public interface BytesBytesCodec extends Codec {

  ByteBuffer encode(ByteBuffer chunkBytes, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  ByteBuffer decode(ByteBuffer chunkBytes, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

}
