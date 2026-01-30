package dev.zarr.zarrjava.utils;

import java.util.Arrays;

public class IndexingUtils {

    public static long[][] computeChunkCoords(long[] arrayShape, int[] chunkShape) {
        return computeChunkCoords(arrayShape, chunkShape, new long[arrayShape.length],
                arrayShape);
    }

    public static long[][] computeChunkCoords(int[] arrayShape, int[] chunkShape) {
        return computeChunkCoords(Utils.toLongArray(arrayShape), chunkShape);
    }

    public static long[][] computeChunkCoords(long[] arrayShape, int[] chunkShape, long[] selOffset,
                                              long[] selShape) {
        final int ndim = arrayShape.length;
        long[] start = new long[ndim];
        long[] end = new long[ndim];
        long numChunks = 1;
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++) {
            final int staIdx = (int) (selOffset[dimIdx] / chunkShape[dimIdx]);
            final int endIdx = (int) ((selOffset[dimIdx] + selShape[dimIdx] - 1) / chunkShape[dimIdx]);
            numChunks *= (endIdx - staIdx + 1);
            start[dimIdx] = staIdx;
            end[dimIdx] = endIdx;
        }

        if (numChunks > Integer.MAX_VALUE) {
            throw new ArithmeticException("Number of chunks exceeds Integer.MAX_VALUE");
        }

        final long[][] chunkCoords = new long[(int) numChunks][];

        final long[] currentIdx = Arrays.copyOf(start, ndim);
        for (int i = 0; i < chunkCoords.length; i++) {
            chunkCoords[i] = Arrays.copyOf(currentIdx, ndim);
            int dimIdx = ndim - 1;
            while (dimIdx >= 0) {
                if (currentIdx[dimIdx] >= end[dimIdx]) {
                    currentIdx[dimIdx] = start[dimIdx];
                    dimIdx--;
                } else {
                    currentIdx[dimIdx]++;
                    dimIdx = -1;
                }
            }
        }
        return chunkCoords;
    }

    public static ChunkProjection computeProjection(long[] chunkCoords, int[] arrayShape,
                                                    int[] chunkShape) {
        return computeProjection(chunkCoords, Utils.toLongArray(arrayShape), chunkShape);
    }

    public static ChunkProjection computeProjection(long[] chunkCoords, long[] arrayShape,
                                                    int[] chunkShape) {
        return computeProjection(chunkCoords, arrayShape, chunkShape, new long[chunkCoords.length],
                arrayShape
        );
    }

    public static ChunkProjection computeProjection(
            final long[] chunkCoords, final long[] arrayShape,
            final int[] chunkShape, final long[] selOffset,
            final long[] selShape
    ) {
        final int ndim = chunkCoords.length;
        final int[] chunkOffset = new int[ndim];
        final int[] outOffset = new int[ndim];
        final int[] shape = new int[ndim];

        for (int dimIdx = 0; dimIdx < chunkCoords.length; dimIdx++) {
            // compute offsets for chunk within overall array
            final long dimOffset = (long) chunkShape[dimIdx] * chunkCoords[dimIdx];
            final long dimLimit = Math.min(arrayShape[dimIdx],
                    (chunkCoords[dimIdx] + 1) * (long) chunkShape[dimIdx]);

            if (selOffset[dimIdx] < dimOffset) {
                // selection starts before current chunk
                chunkOffset[dimIdx] = 0;
                // compute number of previous items, provides offset into output array
                long outOffsetValue = dimOffset - selOffset[dimIdx];
                if (outOffsetValue > Integer.MAX_VALUE) {
                    throw new ArithmeticException(
                            "Output offset exceeds Integer.MAX_VALUE at dimension " + dimIdx + ": " + outOffsetValue);
                }
                outOffset[dimIdx] = (int) outOffsetValue;
            } else {
                // selection starts within current chunk
                long chunkOffsetValue = selOffset[dimIdx] - dimOffset;
                if (chunkOffsetValue > Integer.MAX_VALUE) {
                    throw new ArithmeticException(
                            "Chunk offset exceeds Integer.MAX_VALUE at dimension " + dimIdx + ": " + chunkOffsetValue);
                }
                chunkOffset[dimIdx] = (int) chunkOffsetValue;
                outOffset[dimIdx] = 0;
            }

            if (selOffset[dimIdx] + selShape[dimIdx] > dimLimit) {
                // selection ends after current chunk
                shape[dimIdx] = chunkShape[dimIdx] - chunkOffset[dimIdx];
            } else {
                // selection ends within current chunk
                long shapeValue = selOffset[dimIdx] + selShape[dimIdx] - dimOffset - chunkOffset[dimIdx];
                if (shapeValue > Integer.MAX_VALUE || shapeValue < 0) {
                    throw new ArithmeticException(
                            "Shape value exceeds Integer.MAX_VALUE or is negative at dimension " + dimIdx + ": " + shapeValue);
                }
                shape[dimIdx] = (int) shapeValue;
            }
        }

        return new ChunkProjection(chunkCoords, chunkOffset, outOffset, shape);
    }


    public static long cOrderIndex(final long[] chunkCoords, final long[] arrayShape) {
        long index = 0;
        long multiplier = 1;

        for (int i = arrayShape.length - 1; i >= 0; i--) {
            index += chunkCoords[i] * multiplier;
            multiplier *= arrayShape[i];
        }

        return index;
    }

    public static long fOrderIndex(final long[] chunkCoords, final long[] arrayShape) {
        long index = 0;
        long multiplier = 1;

        for (int i = 0; i < arrayShape.length; i++) {
            index += chunkCoords[i] * multiplier;
            multiplier *= arrayShape[i];
        }

        return index;
    }

    public static boolean isFullChunk(final int[] selOffset, final int[] selShape,
                                      final int[] chunkShape) {
        if (selOffset.length != selShape.length) {
            throw new IllegalArgumentException("'selOffset' and 'selShape' need to have the same rank.");
        }
        if (selOffset.length != chunkShape.length) {
            throw new IllegalArgumentException(
                    "'selOffset' and 'chunkShape' need to have the same rank.");
        }

        for (int dimIdx = 0; dimIdx < selOffset.length; dimIdx++) {
            if (selOffset[dimIdx] != 0 || selShape[dimIdx] != chunkShape[dimIdx]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSingleFullChunk(final long[] selOffset, final long[] selShape,
                                            final int[] chunkShape) {
        if (selOffset.length != selShape.length) {
            throw new IllegalArgumentException("'selOffset' and 'selShape' need to have the same rank.");
        }
        if (selOffset.length != chunkShape.length) {
            throw new IllegalArgumentException(
                    "'selOffset' and 'chunkShape' need to have the same rank.");
        }
        for (int dimIdx = 0; dimIdx < selOffset.length; dimIdx++) {
            if (selOffset[dimIdx] % chunkShape[dimIdx] != 0 || selShape[dimIdx] != chunkShape[dimIdx]) {
                return false;
            }
        }
        return true;
    }

    public static long[] computeSingleChunkCoords(final long[] selOffset, final int[] chunkShape) {
        if (selOffset.length != chunkShape.length) {
            throw new IllegalArgumentException(
                    "'selOffset' and 'chunkShape' need to have the same rank.");
        }
        final long[] chunkCoords = new long[selOffset.length];
        for (int dimIdx = 0; dimIdx < selOffset.length; dimIdx++) {
            chunkCoords[dimIdx] = (selOffset[dimIdx] / chunkShape[dimIdx]);
        }
        return chunkCoords;
    }

    public static final class ChunkProjection {

        final public long[] chunkCoords;
        final public int[] chunkOffset;

        final public int[] outOffset;
        final public int[] shape;

        public ChunkProjection(
                final long[] chunkCoords, final int[] chunkOffset, final int[] outOffset,
                final int[] shape
        ) {
            this.chunkCoords = chunkCoords;
            this.chunkOffset = chunkOffset;
            this.outOffset = outOffset;
            this.shape = shape;
        }

        @Override
        public String toString() {
            return "ChunkProjection{" +
                    "chunkCoords=" + Arrays.toString(chunkCoords) +
                    ", chunkOffset=" + Arrays.toString(chunkOffset) +
                    ", outOffset=" + Arrays.toString(outOffset) +
                    ", shape=" + Arrays.toString(shape) +
                    '}';
        }
    }
}