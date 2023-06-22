package com.scalableminds.zarrjava.indexing;

import com.scalableminds.zarrjava.v3.Utils;
import ucar.ma2.Array;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;

import java.util.ArrayList;
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

    public static ChunkProjection computeProjection(long[] chunkCoords, long[] arrayShape, int[] chunkShape, long[] selOffset, int[] selShape) {
        int ndim = chunkCoords.length;
        int[] chunkOffset = new int[ndim];
        int[] outOffset = new int[ndim];
        int[] shape = new int[ndim];

        for (int dimIdx = 0; dimIdx < chunkCoords.length; dimIdx++) {
            // compute offsets for chunk within overall array
            long dimOffset = (long) chunkShape[dimIdx] * chunkCoords[dimIdx];
            long dimLimit = Math.min(arrayShape[dimIdx], (chunkCoords[dimIdx] + 1) * (long) chunkShape[dimIdx]);
            // determine chunk length, accounting for trailing chunk
            long dimChunkLen = dimLimit - dimOffset;

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


    public static long cOrderIndex(long[] chunkCoords, long[] arrayShape) {
        long index = 0;
        long multiplier = 1;

        for (int i = arrayShape.length - 1; i >= 0; i--) {
            index += chunkCoords[i] * multiplier;
            multiplier *= arrayShape[i];
        }

        return index;
    }

    public static long fOrderIndex(long[] chunkCoords, long[] arrayShape) {
        int index = 0;
        int multiplier = 1;

        for (int i = 0; i < arrayShape.length; i++) {
            index += chunkCoords[i] * multiplier;
            multiplier *= arrayShape[i];
        }

        return index;
    }

    public static void copyRegion(Array source, int[] sourceOffset, Array target, int[] targetOffset, int[] shape) {
        assert sourceOffset.length == targetOffset.length;
        assert source.getRank() == sourceOffset.length;
        assert target.getRank() == targetOffset.length;
        assert shape.length == sourceOffset.length;

        try {
            final ArrayList<Range> sourceRanges = new ArrayList<>();
            final ArrayList<Range> targetRanges = new ArrayList<>();
            for (int dimIdx = 0; dimIdx < shape.length; dimIdx++) {
                assert sourceOffset[dimIdx] + shape[dimIdx] <= source.getShape()[dimIdx];
                assert targetOffset[dimIdx] + shape[dimIdx] <= target.getShape()[dimIdx];

                sourceRanges.add(new Range(sourceOffset[dimIdx], sourceOffset[dimIdx] + shape[dimIdx] - 1));
                targetRanges.add(new Range(targetOffset[dimIdx], targetOffset[dimIdx] + shape[dimIdx] - 1));
            }
            final IndexIterator sourceRangeIterator = source.getRangeIterator(sourceRanges);
            final IndexIterator targetRangeIterator = target.getRangeIterator(targetRanges);
            final Class elementType = source.getElementType();
            ValueSetter setter = createValueSetter(elementType);
            while (sourceRangeIterator.hasNext()) {
                setter.set(sourceRangeIterator, targetRangeIterator);
            }
        } catch (InvalidRangeException ex) {
            throw new RuntimeException("Unreachable");
        }
    }

    private static ValueSetter createValueSetter(Class elementType) {
        if (elementType == double.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setDoubleNext(sourceIterator.getDoubleNext());
        } else if (elementType == float.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setFloatNext(sourceIterator.getFloatNext());
        } else if (elementType == long.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setLongNext(sourceIterator.getLongNext());
        } else if (elementType == int.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setIntNext(sourceIterator.getIntNext());
        } else if (elementType == short.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setShortNext(sourceIterator.getShortNext());
        } else if (elementType == byte.class) {
            return (sourceIterator, targetIterator) -> targetIterator.setByteNext(sourceIterator.getByteNext());
        }
        return (sourceIterator, targetIterator) -> targetIterator.setObjectNext(sourceIterator.getObjectNext());
    }

    private interface ValueSetter {

        void set(IndexIterator sourceIterator, IndexIterator targetIterator);
    }

    public static final class ChunkProjection {
        public long[] chunkCoords;
        public int[] chunkOffset;

        public int[] outOffset;
        public int[] shape;

        public ChunkProjection(long[] chunkCoords, int[] chunkOffset, int[] outOffset, int[] shape) {
            this.chunkCoords = chunkCoords;
            this.chunkOffset = chunkOffset;
            this.outOffset = outOffset;
            this.shape = shape;
        }
    }
}