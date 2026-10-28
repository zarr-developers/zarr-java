package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import ucar.ma2.Array;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

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
         * The inner chunk shape of every nesting level this codec makes addressable, outermost first.
         * <p>
         * A sharding codec contributes its own inner chunk shape and, if its inner chunks are
         * themselves shards, the shapes of the nested sharding codec. The recursion stops as soon as a
         * level's bytes would have to be decoded before its inner units could be located. The number
         * of entries is the number of levels {@link #readInnerChunkDirect} expects coordinates for.
         */
        public abstract int[][] innerChunkShapes();

        /**
         * The shape of the smallest unit that this codec encodes independently inside one stored
         * chunk, i.e. the last entry of {@link #innerChunkShapes()}.
         */
        public int[] innerChunkShape() {
            final int[][] innerChunkShapes = innerChunkShapes();
            return innerChunkShapes[innerChunkShapes.length - 1];
        }

        /**
         * Reads the encoded bytes of a single inner chunk out of a stored chunk, without decoding
         * them.
         *
         * @param handle           the store handle of the stored chunk
         * @param innerChunkCoords the coordinates of the inner chunk, one entry per nesting level of
         *                         {@link #innerChunkShapes()}: first the inner chunk within the stored
         *                         chunk, then the inner chunk within that, and so on
         * @return the encoded inner chunk bytes, or {@code null} if the inner chunk is not present
         */
        @Nullable
        protected abstract ByteBuffer readInnerChunkDirect(
                StoreHandle handle, long[][] innerChunkCoords
        ) throws ZarrException;

        /**
         * Rebuilds a stored chunk so that the given inner chunks hold the given already-encoded bytes,
         * copying every inner chunk that is kept through without decoding it.
         * <p>
         * This is a pure bytes-to-bytes operation; nothing is read from or written to a store.
         *
         * @param chunkBytes the current bytes of the stored chunk, or {@code null} if the stored chunk
         *                   does not exist yet
         * @param updates    the inner chunks to replace or remove, with coordinates relative to the
         *                   stored chunk as in {@link #readInnerChunkDirect}
         * @return the new bytes of the stored chunk, positioned at 0 and sized exactly, or
         *         {@code null} if the stored chunk would hold no inner chunks at all and should
         *         therefore be removed
         */
        @Nullable
        protected abstract ByteBuffer mergeInnerChunksDirect(
                @Nullable ByteBuffer chunkBytes, List<InnerChunkUpdate> updates
        ) throws ZarrException;

        /**
         * The new encoded bytes of one inner chunk, or its removal.
         */
        public static final class InnerChunkUpdate {

            /**
             * The coordinates of the inner chunk relative to the stored chunk, one entry per nesting
             * level as in {@link #readInnerChunkDirect}.
             */
            public final long[][] innerChunkCoords;

            /**
             * The already-encoded inner chunk bytes, or {@code null} to remove the inner chunk.
             */
            @Nullable
            public final ByteBuffer innerChunkBytes;

            public InnerChunkUpdate(long[][] innerChunkCoords, @Nullable ByteBuffer innerChunkBytes) {
                this.innerChunkCoords = innerChunkCoords;
                this.innerChunkBytes = innerChunkBytes;
            }
        }
    }
}

