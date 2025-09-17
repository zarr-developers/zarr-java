package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.IndexingUtils;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.core.codec.CodecPipeline;
import ucar.ma2.InvalidRangeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

public interface Array {

    ArrayMetadata metadata();

    /**
     * Writes a ucar.ma2.Array into the Zarr array at a specified offset. The shape of the Zarr array
     * needs be large enough for the write.
     *
     * @param offset the offset where to write the data
     * @param array the data to write
     * @param parallel utilizes parallelism if true
     */
    default void write(long[] offset, ucar.ma2.Array array, boolean parallel) {
        ArrayMetadata metadata = metadata();
        if (offset.length != metadata.ndim()) {
            throw new IllegalArgumentException("'offset' needs to have rank '" + metadata.ndim() + "'.");
        }
        if (array.getRank() != metadata.ndim()) {
            throw new IllegalArgumentException("'array' needs to have rank '" + metadata.ndim() + "'.");
        }

        int[] shape = array.getShape();

        final int[] chunkShape = metadata.chunkShape();
        Stream<long[]> chunkStream = Arrays.stream(IndexingUtils.computeChunkCoords(metadata.shape(), chunkShape, offset, shape));
        if (parallel) {
            chunkStream = chunkStream.parallel();
        }
        chunkStream.forEach(
            chunkCoords -> {
                try {
                    final IndexingUtils.ChunkProjection chunkProjection =
                        IndexingUtils.computeProjection(chunkCoords, metadata.shape(), chunkShape, offset,
                            shape
                        );

                    ucar.ma2.Array chunkArray;
                    if (IndexingUtils.isFullChunk(chunkProjection.chunkOffset, chunkProjection.shape,
                        chunkShape
                    )) {
                        chunkArray = array.sectionNoReduce(chunkProjection.outOffset,
                            chunkProjection.shape,
                            null
                        );
                    } else {
                        chunkArray = readChunk(chunkCoords);
                        MultiArrayUtils.copyRegion(array, chunkProjection.outOffset, chunkArray,
                            chunkProjection.chunkOffset, chunkProjection.shape
                        );
                    }
                    writeChunk(chunkCoords, chunkArray);
                } catch (ZarrException | InvalidRangeException e) {
                    throw new RuntimeException(e);
                }
            });

    }

    /**
     * Writes one chunk into the Zarr array as specified by the chunk coordinates. The shape of the
     * Zarr array needs to be large enough to write.
     *
     * @param chunkCoords The coordinates of the chunk as computed by the offset of the chunk divided
     *                    by the chunk shape.
     * @param chunkArray The data to write into the chunk
     * @throws ZarrException throws ZarrException if the write fails
     */
    default void writeChunk(long[] chunkCoords, ucar.ma2.Array chunkArray) throws ZarrException {
        ArrayMetadata metadata = metadata();
        String[] chunkKeys = metadata.chunkKeyEncoding().encodeChunkKey(chunkCoords);
        StoreHandle chunkHandle = storeHandle().resolve(chunkKeys);

        if (MultiArrayUtils.allValuesEqual(chunkArray, metadata.parsedFillValue())) {
            chunkHandle.delete();
        } else {
            ByteBuffer chunkBytes = codecPipeline().encode(chunkArray);
            chunkHandle.set(chunkBytes);
        }
    }

    /**
     * Reads one chunk of the Zarr array as specified by the chunk coordinates into an
     * ucar.ma2.Array.
     *
     * @param chunkCoords The coordinates of the chunk as computed by the offset of the chunk divided
     *                    by the chunk shape.
     * @throws ZarrException throws ZarrException if the requested chunk is outside the array's domain or if the read fails
     */
    @Nonnull
    default ucar.ma2.Array readChunk(long[] chunkCoords)
        throws ZarrException {
        ArrayMetadata metadata = metadata();
        if (!chunkIsInArray(chunkCoords)) {
            throw new ZarrException("Attempting to read data outside of the array's domain.");
        }

        final String[] chunkKeys = metadata.chunkKeyEncoding().encodeChunkKey(chunkCoords);
        final StoreHandle chunkHandle = storeHandle().resolve(chunkKeys);

        ByteBuffer chunkBytes = chunkHandle.read();
        if (chunkBytes == null) {
            return metadata.allocateFillValueChunk();
        }

        return codecPipeline().decode(chunkBytes);
    }


    /**
     * Writes a ucar.ma2.Array into the Zarr array at the beginning of the Zarr array. The shape of
     * the Zarr array needs be large enough for the write.
     * Utilizes no parallelism.
     *
     * @param array the data to write
     */
    default void write(ucar.ma2.Array array) {
        write(new long[metadata().ndim()], array);
    }

    /**
     * Writes a ucar.ma2.Array into the Zarr array at a specified offset. The shape of the Zarr array
     * needs be large enough for the write.
     * Utilizes no parallelism.
     *
     * @param offset the offset where to write the data
     * @param array the data to write
     */
    default void write(long[] offset, ucar.ma2.Array array) {
        write(offset, array, false);
    }

    /**
     * Writes a ucar.ma2.Array into the Zarr array at the beginning of the Zarr array. The shape of
     * the Zarr array needs be large enough for the write.
     *
     * @param array the data to write
     * @param parallel utilizes parallelism if true
     */
    default void write(ucar.ma2.Array array, boolean parallel) {
        write(new long[metadata().ndim()], array, parallel);
    }

    /**
     * Reads the entire Zarr array into an ucar.ma2.Array.
     * Utilizes no parallelism.
     *
     * @throws ZarrException throws ZarrException if the read fails
     */
    @Nonnull
    default ucar.ma2.Array read() throws ZarrException {
        return read(new long[metadata().ndim()], Utils.toIntArray(metadata().shape()));
    }

    /**
     * Reads a part of the Zarr array based on a requested offset and shape into an ucar.ma2.Array.
     * Utilizes no parallelism.
     *
     * @param offset the offset where to start reading
     * @param shape the shape of the data to read
     * @throws ZarrException throws ZarrException if the requested data is outside the array's domain or if the read fails
     */
    @Nonnull
    default ucar.ma2.Array read(final long[] offset, final int[] shape) throws ZarrException {
        return read(offset, shape, false);
    }

    /**
     * Reads the entire Zarr array into an ucar.ma2.Array.
     *
     * @param parallel utilizes parallelism if true
     * @throws ZarrException throws ZarrException if the requested data is outside the array's domain or if the read fails
     */
    @Nonnull
    default ucar.ma2.Array read(final boolean parallel) throws ZarrException {
        return read(new long[metadata().ndim()], Utils.toIntArray(metadata().shape()), parallel);
    }

    default boolean chunkIsInArray(long[] chunkCoords) {
        final int[] chunkShape = metadata().chunkShape();
        for (int dimIdx = 0; dimIdx < metadata().ndim(); dimIdx++) {
            if (chunkCoords[dimIdx] < 0
                || chunkCoords[dimIdx] * chunkShape[dimIdx] >= metadata().shape()[dimIdx]) {
                return false;
            }
        }
        return true;
    }


    StoreHandle storeHandle();

    CodecPipeline codecPipeline();

    /**
     * Reads a part of the Zarr array based on a requested offset and shape into an ucar.ma2.Array.
     *
     * @param offset the offset where to start reading
     * @param shape the shape of the data to read
     * @param parallel utilizes parallelism if true
     * @throws ZarrException throws ZarrException if the requested data is outside the array's domain or if the read fails
     */
    @Nonnull
    default ucar.ma2.Array read(final long[] offset, final int[] shape, final boolean parallel) throws ZarrException {
        ArrayMetadata metadata = metadata();
        CodecPipeline codecPipeline = codecPipeline();
        if (offset.length != metadata.ndim()) {
            throw new IllegalArgumentException("'offset' needs to have rank '" + metadata.ndim() + "'.");
        }
        if (shape.length != metadata.ndim()) {
            throw new IllegalArgumentException("'shape' needs to have rank '" + metadata.ndim() + "'.");
        }
        for (int dimIdx = 0; dimIdx < metadata.ndim(); dimIdx++) {
            if (offset[dimIdx] < 0 || offset[dimIdx] + shape[dimIdx] > metadata.shape()[dimIdx]) {
                throw new ZarrException("Requested data is outside of the array's domain.");
            }
        }

        final int[] chunkShape = metadata.chunkShape();
        if (IndexingUtils.isSingleFullChunk(offset, shape, chunkShape)) {
            return readChunk(IndexingUtils.computeSingleChunkCoords(offset, chunkShape));
        }

        final ucar.ma2.Array outputArray = ucar.ma2.Array.factory(metadata.dataType().getMA2DataType(),
            shape);
        Stream<long[]> chunkStream = Arrays.stream(IndexingUtils.computeChunkCoords(metadata.shape(), chunkShape, offset, shape));
        if (parallel) {
            chunkStream = chunkStream.parallel();
        }
        chunkStream.forEach(
            chunkCoords -> {
                try {
                    final IndexingUtils.ChunkProjection chunkProjection =
                        IndexingUtils.computeProjection(chunkCoords, metadata.shape(), chunkShape, offset,
                            shape
                        );

                    if (chunkIsInArray(chunkCoords)) {
                        MultiArrayUtils.copyRegion(metadata.allocateFillValueChunk(),
                            chunkProjection.chunkOffset, outputArray, chunkProjection.outOffset,
                            chunkProjection.shape
                        );
                    }

                    final String[] chunkKeys = metadata.chunkKeyEncoding().encodeChunkKey(chunkCoords);
                    final StoreHandle chunkHandle = storeHandle().resolve(chunkKeys);
                    if (!chunkHandle.exists()) {
                        return;
                    }
                    if (codecPipeline.supportsPartialDecode()) {
                        final ucar.ma2.Array chunkArray = codecPipeline.decodePartial(chunkHandle,
                            Utils.toLongArray(chunkProjection.chunkOffset), chunkProjection.shape);
                        MultiArrayUtils.copyRegion(chunkArray, new int[metadata.ndim()], outputArray,
                            chunkProjection.outOffset, chunkProjection.shape
                        );
                    } else {
                        MultiArrayUtils.copyRegion(readChunk(chunkCoords), chunkProjection.chunkOffset,
                            outputArray, chunkProjection.outOffset, chunkProjection.shape
                        );
                    }

                } catch (ZarrException e) {
                    throw new RuntimeException(e);
                }
            });
        return outputArray;
    }

    default ArrayAccessor access() {
        return new ArrayAccessor(this);
    }

    final class ArrayAccessor {
        @Nullable
        long[] offset;
        @Nullable
        int[] shape;
        @Nonnull
        Array array;

        public ArrayAccessor(@Nonnull Array array) {
            this.array = array;
        }

        @Nonnull
        public ArrayAccessor withOffset(@Nonnull long... offset) {
            this.offset = offset;
            return this;
        }


        @Nonnull
        public ArrayAccessor withShape(@Nonnull int... shape) {
            this.shape = shape;
            return this;
        }

        @Nonnull
        public ArrayAccessor withShape(@Nonnull long... shape) {
            this.shape = Utils.toIntArray(shape);
            return this;
        }

        @Nonnull
        public ucar.ma2.Array read() throws ZarrException {
            if (offset == null) {
                throw new ZarrException("`offset` needs to be set.");
            }
            if (shape == null) {
                throw new ZarrException("`shape` needs to be set.");
            }
            return array.read(offset, shape);
        }

        public void write(@Nonnull ucar.ma2.Array content) throws ZarrException {
            if (offset == null) {
                throw new ZarrException("`offset` needs to be set.");
            }
            array.write(offset, content);
        }

    }
}