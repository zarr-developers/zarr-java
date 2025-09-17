package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import ucar.ma2.Array;

public interface ArrayArrayCodec {

  Array encode(Array chunkArray)
      throws ZarrException;

  Array decode(Array chunkArray)
      throws ZarrException;

}
