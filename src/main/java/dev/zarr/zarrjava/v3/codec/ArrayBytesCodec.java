package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata.CoreArrayMetadata;
import java.nio.ByteBuffer;
import ucar.ma2.Array;

public interface ArrayBytesCodec extends Codec {

  ByteBuffer encode(Array chunkArray, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  Array decode(ByteBuffer chunkBytes, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  interface WithPartialDecode extends ArrayBytesCodec {

    Array decodePartial(
        StoreHandle handle, long[] offset, int[] shape,
        CoreArrayMetadata arrayMetadata
    ) throws ZarrException;
  }
}

