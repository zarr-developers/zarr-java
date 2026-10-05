package dev.zarr.zarrjava.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteRangeCoalescerTest {

    @Test
    public void testMergesNearbyRangesInStartOrder() {
        List<int[]> groups = ByteRangeCoalescer.coalesce(
                new long[]{9000, 100, 210, 305}, new long[]{9100, 200, 300, 400}, 10, 1 << 20);
        Assertions.assertEquals(2, groups.size());
        Assertions.assertArrayEquals(new int[]{1, 2, 3}, groups.get(0));
        Assertions.assertArrayEquals(new int[]{0}, groups.get(1));
    }

    @Test
    public void testRespectsGapLimit() {
        // gap of exactly maxGapBytes merges, one byte more does not
        Assertions.assertEquals(1, ByteRangeCoalescer.coalesce(
                new long[]{0, 20}, new long[]{10, 30}, 10, 1 << 20).size());
        Assertions.assertEquals(2, ByteRangeCoalescer.coalesce(
                new long[]{0, 21}, new long[]{10, 31}, 10, 1 << 20).size());
    }

    @Test
    public void testRespectsSizeLimit() {
        List<int[]> groups = ByteRangeCoalescer.coalesce(
                new long[]{0, 10, 20}, new long[]{10, 20, 30}, 0, 20);
        Assertions.assertEquals(2, groups.size());
        Assertions.assertArrayEquals(new int[]{0, 1}, groups.get(0));
        Assertions.assertArrayEquals(new int[]{2}, groups.get(1));
    }

    @Test
    public void testMergesOverlappingRanges() {
        List<int[]> groups = ByteRangeCoalescer.coalesce(
                new long[]{0, 5, 2}, new long[]{50, 10, 60}, 0, 1 << 20);
        Assertions.assertEquals(1, groups.size());
        Assertions.assertArrayEquals(new int[]{0, 2, 1}, groups.get(0));
    }

    @Test
    public void testRejectsInvalidRanges() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ByteRangeCoalescer.validate(new long[]{0}, new long[]{10, 20}));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ByteRangeCoalescer.validate(new long[]{-1}, new long[]{10}));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ByteRangeCoalescer.validate(new long[]{10}, new long[]{5}));
    }

    @Test
    public void testDefaultGetRangesIssuesOneGetPerGroup() {
        List<long[]> calls = new ArrayList<>();
        MemoryStore store = new MemoryStore() {
            @Override
            public ByteBuffer get(String[] keys, long start, long end) {
                calls.add(new long[]{start, end});
                return super.get(keys, start, end);
            }
        };
        byte[] data = new byte[10_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        store.set(new String[]{"shard"}, ByteBuffer.wrap(data));

        ByteBuffer[] ranges = store.getRanges(new String[]{"shard"},
                new long[]{9000, 100, 210, 305}, new long[]{9100, 200, 300, 400}, 10, 1 << 20);

        Assertions.assertEquals(2, calls.size());
        Assertions.assertArrayEquals(new long[]{100, 400}, calls.get(0));
        Assertions.assertArrayEquals(new long[]{9000, 9100}, calls.get(1));
        Assertions.assertEquals(ByteBuffer.wrap(data, 9000, 100), ranges[0]);
        Assertions.assertEquals(ByteBuffer.wrap(data, 100, 100), ranges[1]);
        Assertions.assertEquals(ByteBuffer.wrap(data, 210, 90), ranges[2]);
        Assertions.assertEquals(ByteBuffer.wrap(data, 305, 95), ranges[3]);
    }
}
