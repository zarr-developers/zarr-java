package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata.CoreArrayMetadata;
import ucar.ma2.Array;

public interface ArrayArrayCodec extends Codec {

  Array encode(Array chunkArray, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  Array decode(Array chunkArray, CoreArrayMetadata arrayMetadata)
      throws ZarrException;

}
