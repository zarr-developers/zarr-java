package dev.zarr.zarrjava.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Plans which byte ranges of one value can be fetched together, mirroring zarr-python's
 * {@code zarr.core._coalesce.coalesce_ranges}.
 */
final class ByteRangeCoalescer {

    private ByteRangeCoalescer() {
    }

    /**
     * Checks that {@code starts} and {@code ends} describe well-formed ranges {@code [starts[i], ends[i])}.
     */
    static void validate(long[] starts, long[] ends) {
        if (starts.length != ends.length) {
            throw new IllegalArgumentException("'starts' and 'ends' need to have the same length.");
        }
        for (int i = 0; i < starts.length; i++) {
            if (starts[i] < 0 || ends[i] < starts[i]) {
                throw new IllegalArgumentException(
                        String.format("Invalid byte range [%d, %d).", starts[i], ends[i]));
            }
        }
    }

    /**
     * Groups the ranges {@code [starts[i], ends[i])} so that each group can be served by a single
     * fetch. Ranges are sorted by start; a range joins the current group if the gap to the group's
     * running end is at most {@code maxGapBytes} and the merged span stays within
     * {@code maxCoalescedBytes}.
     *
     * @return the groups as lists of input indices, each sorted by start
     */
    static List<int[]> coalesce(long[] starts, long[] ends, long maxGapBytes, long maxCoalescedBytes) {
        Integer[] order = new Integer[starts.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        Arrays.sort(order, Comparator.comparingLong(i -> starts[i]));

        List<int[]> groups = new ArrayList<>();
        List<Integer> group = new ArrayList<>();
        long groupStart = 0;
        long groupEnd = 0;
        for (int i : order) {
            if (!group.isEmpty() && starts[i] - groupEnd <= maxGapBytes) {
                long prospectiveEnd = Math.max(groupEnd, ends[i]);
                if (prospectiveEnd - groupStart <= maxCoalescedBytes) {
                    group.add(i);
                    groupEnd = prospectiveEnd;
                    continue;
                }
            }
            if (!group.isEmpty()) {
                groups.add(group.stream().mapToInt(Integer::intValue).toArray());
            }
            group = new ArrayList<>();
            group.add(i);
            groupStart = starts[i];
            groupEnd = ends[i];
        }
        if (!group.isEmpty()) {
            groups.add(group.stream().mapToInt(Integer::intValue).toArray());
        }
        return groups;
    }
}
