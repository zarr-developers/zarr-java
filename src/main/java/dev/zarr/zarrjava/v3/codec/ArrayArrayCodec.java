package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import ucar.ma2.Array;

public abstract class ArrayArrayCodec extends Codec {

  protected abstract Array encode(Array chunkArray)
      throws ZarrException;

  protected abstract Array decode(Array chunkArray)
      throws ZarrException;

}
