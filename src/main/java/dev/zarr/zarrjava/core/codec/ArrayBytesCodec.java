package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import ucar.ma2.Array;

import javax.annotation.Nullable;
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

        /**
         * The shape of the smallest unit that this codec encodes independently inside one stored
         * chunk, and that {@link #readInnerChunkEncoded} can therefore address.
         */
        public abstract int[] innerChunkShape();

        /**
         * Reads the encoded bytes of a single inner chunk out of a stored chunk, without decoding
         * them.
         *
         * @param handle          the store handle of the stored chunk
         * @param innerChunkCoords the coordinates of the inner chunk relative to the stored chunk, on
         *                         the grid given by {@link #innerChunkShape()}
         * @return the encoded inner chunk bytes, or {@code null} if the inner chunk is not present
         */
        @Nullable
        protected abstract ByteBuffer readInnerChunkEncoded(
                StoreHandle handle, long[] innerChunkCoords
        ) throws ZarrException;
    }
}

