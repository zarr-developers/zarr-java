package dev.zarr.zarrjava.indexing;

import java.util.Arrays;

public class Indexer {
    public static long[][] computeChunkCoords(long[] shape, int[] chunkShape, int[] bufferShape, long[] offset) {
        final int depth = shape.length;
        long[] start = new long[depth];
        long[] end = new long[depth];
        int numChunks = 1;
        for (int i = 0; i < depth; i++) {
            final int staIdx = (int)(offset[i] / chunkShape[i]);
            final int endIdx = (int)((offset[i] + bufferShape[i] - 1) / chunkShape[i]);
            numChunks *= (endIdx - staIdx + 1);
            start[i] = staIdx;
            end[i] = endIdx;
        }

        final long[][] chunkIndices = new long[numChunks][];

        final long[] currentIdx = Arrays.copyOf(start, depth);
        for (int i = 0; i < chunkIndices.length; i++) {
            chunkIndices[i] = Arrays.copyOf(currentIdx, depth);
            int depthIdx = depth - 1;
            while (depthIdx >= 0) {
                if (currentIdx[depthIdx] >= end[depthIdx]) {
                    currentIdx[depthIdx] = start[depthIdx];
                    depthIdx--;
                } else {
                    currentIdx[depthIdx]++;
                    depthIdx = -1;
                }
            }
        }
        return chunkIndices;
    }
}