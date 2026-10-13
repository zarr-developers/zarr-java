package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import ucar.ma2.Array;

public abstract class ArrayArrayCodec extends AbstractCodec {

    public abstract Array encode(Array chunkArray)
            throws ZarrException;

    public abstract Array decode(Array chunkArray)
            throws ZarrException;

}
