package com.scalableminds.zarrjava.indexing;

import com.scalableminds.zarrjava.v3.Utils;

import java.util.Arrays;

public class Indexer {

    public static long[][] computeChunkCoords(long[] arrayShape, int[] chunkShape) {
        return computeChunkCoords(arrayShape, chunkShape, new long[arrayShape.length], Utils.toIntArray(arrayShape));
    }

    public static long[][] computeChunkCoords(int[] arrayShape, int[] chunkShape) {
        return computeChunkCoords(Utils.toLongArray(arrayShape), chunkShape);
    }

    public static long[][] computeChunkCoords(long[] arrayShape, int[] chunkShape, long[] selOffset, int[] selShape) {
        final int ndim = arrayShape.length;
        long[] start = new long[ndim];
        long[] end = new long[ndim];
        int numChunks = 1;
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++) {
            final int staIdx = (int) (selOffset[dimIdx] / chunkShape[dimIdx]);
            final int endIdx = (int) ((selOffset[dimIdx] + selShape[dimIdx] - 1) / chunkShape[dimIdx]);
            numChunks *= (endIdx - staIdx + 1);
            start[dimIdx] = staIdx;
            end[dimIdx] = endIdx;
        }

        final long[][] chunkCoords = new long[numChunks][];

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

    public static ChunkProjection computeProjection(long[] chunkCoords, int[] arrayShape, int[] chunkShape) {
        return computeProjection(chunkCoords, Utils.toLongArray(arrayShape), chunkShape);
    }

    public static ChunkProjection computeProjection(long[] chunkCoords, long[] arrayShape, int[] chunkShape) {
        return computeProjection(chunkCoords, arrayShape, chunkShape, new long[chunkCoords.length],
                Utils.toIntArray(arrayShape));
    }

    public static ChunkProjection computeProjection(final long[] chunkCoords, final long[] arrayShape, final int[] chunkShape, final long[] selOffset, final int[] selShape) {
        final int ndim = chunkCoords.length;
        final int[] chunkOffset = new int[ndim];
        final int[] outOffset = new int[ndim];
        final int[] shape = new int[ndim];

        for (int dimIdx = 0; dimIdx < chunkCoords.length; dimIdx++) {
            // compute offsets for chunk within overall array
            final long dimOffset = (long) chunkShape[dimIdx] * chunkCoords[dimIdx];
            final long dimLimit = Math.min(arrayShape[dimIdx], (chunkCoords[dimIdx] + 1) * (long) chunkShape[dimIdx]);

            if (selOffset[dimIdx] < dimOffset) {
                // selection starts before current chunk
                chunkOffset[dimIdx] = 0;
                // compute number of previous items, provides offset into output array
                outOffset[dimIdx] = (int) (dimOffset - selOffset[dimIdx]);
            } else {
                // selection starts within current chunk
                chunkOffset[dimIdx] = (int) (selOffset[dimIdx] - dimOffset);
                outOffset[dimIdx] = 0;
            }

            if (selOffset[dimIdx] + selShape[dimIdx] > dimLimit) {
                // selection ends after current chunk
                shape[dimIdx] = (int) (chunkShape[dimIdx] - selOffset[dimIdx]);
            } else {
                // selection ends within current chunk
                shape[dimIdx] = (int) (selOffset[dimIdx] + selShape[dimIdx] - dimOffset - chunkOffset[dimIdx]);
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
        int index = 0;
        int multiplier = 1;

        for (int i = 0; i < arrayShape.length; i++) {
            index += chunkCoords[i] * multiplier;
            multiplier *= arrayShape[i];
        }

        return index;
    }

    public static boolean isFullChunk(final int[] selOffset, final int[] selShape, final int[] chunkShape) {
        assert selOffset.length == selShape.length;
        assert selOffset.length == chunkShape.length;

        for (int dimIdx = 0; dimIdx < selOffset.length; dimIdx++) {
            if (selOffset[dimIdx] != 0 || selShape[dimIdx] != chunkShape[dimIdx]) {
                return false;
            }
        }
        return true;
    }

    public static final class ChunkProjection {
        final public long[] chunkCoords;
        final public int[] chunkOffset;

        final public int[] outOffset;
        final public int[] shape;

        public ChunkProjection(final long[] chunkCoords, final int[] chunkOffset, final int[] outOffset, final int[] shape) {
            this.chunkCoords = chunkCoords;
            this.chunkOffset = chunkOffset;
            this.outOffset = outOffset;
            this.shape = shape;
        }
    }
}