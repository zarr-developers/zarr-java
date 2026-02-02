package dev.zarr.zarrjava;


import dev.zarr.zarrjava.utils.IndexingUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;

import static dev.zarr.zarrjava.utils.IndexingUtils.computeChunkCoords;
import static dev.zarr.zarrjava.utils.Utils.inversePermutation;
import static dev.zarr.zarrjava.utils.Utils.isPermutation;

public class TestUtils {
    @Test
    public void testIsPermutation() {
        assert isPermutation(new int[]{2, 1, 0});
        assert isPermutation(new int[]{4, 2, 1, 3, 0});
        assert !isPermutation(new int[]{0, 1, 2, 0});
        assert !isPermutation(new int[]{0, 1, 2, 3, 5});
        assert !isPermutation(new int[]{});
    }

    @Test
    public void testInversePermutation() {
        Assertions.assertArrayEquals(new int[]{1, 0, 2}, inversePermutation(new int[]{1, 0, 2}));
        Assertions.assertArrayEquals(new int[]{2, 0, 1}, inversePermutation(new int[]{1, 2, 0}));
        Assertions.assertArrayEquals(new int[]{0, 3, 2, 4, 1}, inversePermutation(new int[]{0, 4, 2, 1, 3}));
        Assertions.assertFalse(Arrays.equals(new int[]{2, 0, 1}, inversePermutation(new int[]{2, 0, 1})));
    }

    @Test
    public void testComputeChunkCoords(){
        long[] arrayShape = new long[]{100, 100};
        int[] chunkShape = new int[]{30, 30};
        long[] selOffset = new long[]{50, 20};
        long[] selShape = new long[]{20, 1};
        long[][] chunkCoords = computeChunkCoords(arrayShape, chunkShape, selOffset, selShape);
        long[][] expectedChunkCoords = new long[][]{
                {1, 0},
                {2, 0},
        };
        Assertions.assertArrayEquals(expectedChunkCoords, chunkCoords);

        arrayShape = new long[]{1, 52};
        chunkShape = new int[]{1, 17};
        selOffset = new long[]{0, 32};
        selShape = new long[]{1, 20};
        chunkCoords = computeChunkCoords(arrayShape, chunkShape, selOffset, selShape);
        expectedChunkCoords = new long[][]{
                {0, 1},
                {0, 2},
                {0, 3},
        };
        Assertions.assertArrayEquals(expectedChunkCoords, chunkCoords);
    }

    @Test
    public void testComputeProjection(){
        // chunk (0,2) contains indexes 34-50 along axis 1
        // thus the overlap with selection 32-52 is 34-50
        // which is offset 2 in the selection and offset 0 in the chunk
        // and has full chunk length 17
        final long[] chunkCoords = new long[]{0, 2};
        final long[] arrayShape = new long[]{1, 52};
        final int[] chunkShape = new int[]{1, 17};
        final long[] selOffset = new long[]{0, 32};
        final long[] selShape = new long[]{1, 20};

        IndexingUtils.ChunkProjection projection = IndexingUtils.computeProjection(
                chunkCoords, arrayShape, chunkShape, selOffset, selShape
        );
        Assertions.assertArrayEquals(chunkCoords, projection.chunkCoords);
        Assertions.assertArrayEquals(new int[]{0,0}, projection.chunkOffset);
        Assertions.assertArrayEquals(new int[]{0,2}, projection.outOffset);
        Assertions.assertArrayEquals(new int[]{1, 17}, projection.shape);
    }

    @Test
    public void testComputeChunkCoordsOverflow() {
        // Shape: [100000, 100000]
        long[] arrayShape = {100000, 100000};
        // Chunk: [1, 1]
        int[] chunkShape = {1, 1};
        // Selection: Full array
        long[] selOffset = {0, 0};
        long[] selShape = {100000, 100000};

        // This should cause overflow: 100000 * 100000 = 10,000,000,000 > Integer.MAX_VALUE
        Assertions.assertThrows(ArithmeticException.class, () -> {
            IndexingUtils.computeChunkCoords(arrayShape, chunkShape, selOffset, selShape);
        });
    }

}

