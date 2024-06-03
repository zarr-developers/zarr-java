package dev.zarr.zarrjava;


import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;

import static dev.zarr.zarrjava.utils.Utils.inversePermutation;
import static dev.zarr.zarrjava.utils.Utils.isPermutation;
import static org.junit.Assert.assertFalse;

public class TestUtils {
    @Test
    public void testIsPermutation(){
        assert isPermutation(new int[]{2, 1, 0});
        assert isPermutation(new int[]{4, 2, 1, 3, 0});
        assert !isPermutation(new int[]{0, 1, 2, 0});
        assert !isPermutation(new int[]{0, 1, 2, 3, 5});
        assert !isPermutation(new int[]{});
    }

    @Test
    public void testInversePermutation(){
        Assertions.assertArrayEquals(new int[]{1, 0, 2}, inversePermutation(new int[]{1, 0, 2}));
        Assertions.assertArrayEquals(new int[]{2, 0, 1}, inversePermutation(new int[]{1, 2, 0}));
        Assertions.assertArrayEquals(new int[]{0, 3, 2, 4, 1}, inversePermutation(new int[]{0, 4, 2, 1, 3}));
        Assertions.assertFalse(Arrays.equals(new int[]{2, 0, 1}, inversePermutation(new int[]{2, 0, 1})));
    }

}

