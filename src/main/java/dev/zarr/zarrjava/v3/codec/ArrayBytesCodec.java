package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import java.nio.ByteBuffer;
import ucar.ma2.Array;

public abstract class ArrayBytesCodec extends Codec {

  protected abstract ByteBuffer encode(Array chunkArray)
      throws ZarrException;

  protected abstract Array decode(ByteBuffer chunkBytes)
      throws ZarrException;

  public abstract static class WithPartialDecode extends ArrayBytesCodec {

    public abstract Array decode(ByteBuffer shardBytes) throws ZarrException;
    public abstract ByteBuffer encode(Array shardArray) throws ZarrException;

    protected abstract Array decodePartial(
        StoreHandle handle, long[] offset, int[] shape
    ) throws ZarrException;
  }
}

