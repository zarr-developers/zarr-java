package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import java.nio.ByteBuffer;
import ucar.ma2.Array;

public interface ArrayBytesCodec {

  ByteBuffer encode(Array chunkArray)
      throws ZarrException;

  Array decode(ByteBuffer chunkBytes)
      throws ZarrException;

  interface WithPartialDecode extends ArrayBytesCodec {

    public abstract Array decode(ByteBuffer shardBytes) throws ZarrException;
    public abstract ByteBuffer encode(Array shardArray) throws ZarrException;

     Array decodePartial(
        StoreHandle handle, long[] offset, int[] shape
    ) throws ZarrException;
  }
}

