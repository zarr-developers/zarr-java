package dev.zarr.zarrjava.v2.codec.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.core.codec.ArrayArrayCodec;
import dev.zarr.zarrjava.v2.codec.Codec;
import ucar.ma2.Array;

/**
 * Realizes the Fortran (column-major) chunk layout that Zarr v2 selects with {@code "order": "F"}.
 * <p>
 * Zarr v2 stores the elements of a chunk either in row-major ({@code "C"}) or column-major
 * ({@code "F"}) order. Zarr v3 dropped the {@code order} key and expresses the same thing with the
 * {@code transpose} codec, so the v2 {@code order} key is modelled here the same way: as an
 * array &rarr; array codec that reverses all axes. The row-major serialization of an array with
 * reversed axes is exactly the column-major serialization of the original array.
 * <p>
 * This codec is never part of the {@code .zarray} document. It is inserted into the codec pipeline
 * by {@link dev.zarr.zarrjava.v2.Array} whenever the metadata says {@code "order": "F"}, and the
 * {@code order} key alone remains the on-disk representation.
 */
public class FortranOrderCodec extends ArrayArrayCodec implements Codec {

    /**
     * Reversing all axes is its own inverse, so encoding and decoding differ only in what they do
     * with the resulting view.
     */
    private Array reverseAxes(Array chunkArray) throws ZarrException {
        int ndim = resolveArrayMetadata().ndim();
        if (chunkArray.getRank() != ndim) {
            throw new ZarrException(
                    "Chunk array has rank " + chunkArray.getRank() + ", but the array has " + ndim
                            + " dimensions.");
        }
        int[] reversedAxes = new int[ndim];
        for (int i = 0; i < ndim; i++) {
            reversedAxes[i] = ndim - 1 - i;
        }
        return chunkArray.permute(reversedAxes);
    }

    /**
     * Turns the logical chunk into the axis-reversed view that the following
     * {@link dev.zarr.zarrjava.core.codec.core.BytesCodec} then serializes row-major, which yields
     * the column-major serialization of the logical chunk. The view is handed on as-is: the
     * {@code BytesCodec} flattens in logical index order, so no copy is needed here.
     */
    @Override
    public Array encode(Array chunkArray) throws ZarrException {
        return reverseAxes(chunkArray);
    }

    /**
     * Turns the axis-reversed array that came out of the
     * {@link dev.zarr.zarrjava.core.codec.core.BytesCodec} back into the logical chunk.
     * <p>
     * The result is materialized with {@link Array#copy()} rather than returned as a permuted view.
     * A view's logical order is correct for iterators and index-based access, but its backing store
     * is still in column-major order, so linear accessors such as {@code getInt(int)} would return
     * elements in the wrong order. Chunks decoded here can be handed straight to callers by the
     * single-full-chunk fast path in {@link dev.zarr.zarrjava.core.Array#read(long[], long[])}, so
     * they have to behave like any other decoded chunk.
     */
    @Override
    public Array decode(Array chunkArray) throws ZarrException {
        return reverseAxes(chunkArray).copy();
    }

    /**
     * Reports the axis-reversed shapes to the following codec, so that the
     * {@link dev.zarr.zarrjava.core.codec.core.BytesCodec} reads and writes the chunk bytes with the
     * reversed chunk shape.
     */
    @Override
    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        super.resolveArrayMetadata();
        int ndim = arrayMetadata.ndim();
        long[] reversedShape = new long[ndim];
        int[] reversedChunkShape = new int[ndim];
        for (int i = 0; i < ndim; i++) {
            reversedShape[i] = arrayMetadata.shape[ndim - 1 - i];
            reversedChunkShape[i] = arrayMetadata.chunkShape[ndim - 1 - i];
        }
        return new ArrayMetadata.CoreArrayMetadata(
                reversedShape,
                reversedChunkShape,
                arrayMetadata.dataType,
                arrayMetadata.parsedFillValue
        );
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return inputByteLength;
    }

    @Override
    public Codec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return this;
    }
}
