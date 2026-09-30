package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import ucar.ma2.Array;

import java.nio.ByteBuffer;

public abstract class ArrayBytesCodec extends AbstractCodec {

    public abstract ByteBuffer encode(Array chunkArray)
            throws ZarrException;

    public abstract Array decode(ByteBuffer chunkBytes)
            throws ZarrException;

    public abstract static class WithPartialDecode extends ArrayBytesCodec {

        public abstract Array decode(ByteBuffer shardBytes) throws ZarrException;

        public abstract ByteBuffer encode(Array shardArray) throws ZarrException;

        protected abstract Array decodePartial(
                StoreHandle handle, long[] offset, int[] shape
        ) throws ZarrException;
    }
}

