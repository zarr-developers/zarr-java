package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

public interface Store {

    boolean exists(String[] keys);

    @Nullable
    ByteBuffer get(String[] keys);

    @Nullable
    ByteBuffer get(String[] keys, long start);

    @Nullable
    ByteBuffer get(String[] keys, long start, long end);

    /**
     * Default for the {@code maxGapBytes} argument of {@link #getRanges(String[], long[], long[], long, long)}
     * (1 MiB, as in zarr-python).
     */
    long DEFAULT_MAX_GAP_BYTES = 1 << 20;

    /**
     * Default for the {@code maxCoalescedBytes} argument of
     * {@link #getRanges(String[], long[], long[], long, long)} (16 MiB, as in zarr-python).
     */
    long DEFAULT_MAX_COALESCED_BYTES = 16 << 20;

    /**
     * Reads many byte ranges of the value at the given keys, using the default coalescing limits.
     *
     * @see #getRanges(String[], long[], long[], long, long)
     */
    @Nullable
    default ByteBuffer[] getRanges(String[] keys, long[] starts, long[] ends) {
        return getRanges(keys, starts, ends, DEFAULT_MAX_GAP_BYTES, DEFAULT_MAX_COALESCED_BYTES);
    }

    /**
     * Reads many byte ranges {@code [starts[i], ends[i])} of the value at the given keys.
     * <p>
     * The default implementation coalesces nearby ranges into fewer {@link #get(String[], long, long)}
     * calls and slices the results back apart. Stores with a cheaper way to read many ranges may
     * override it.
     *
     * @param keys              the keys identifying the value
     * @param starts            the start offsets (inclusive) of the ranges
     * @param ends              the end offsets (exclusive) of the ranges
     * @param maxGapBytes       two ranges separated by at most this many bytes may be fetched together
     * @param maxCoalescedBytes upper bound on the size of a single coalesced fetch
     * @return one buffer per range, in input order, or null if the value does not exist
     */
    @Nullable
    default ByteBuffer[] getRanges(String[] keys, long[] starts, long[] ends,
                                   long maxGapBytes, long maxCoalescedBytes) {
        ByteRangeCoalescer.validate(starts, ends);
        ByteBuffer[] result = new ByteBuffer[starts.length];
        for (int[] group : ByteRangeCoalescer.coalesce(starts, ends, maxGapBytes, maxCoalescedBytes)) {
            long groupStart = starts[group[0]];
            long groupEnd = Arrays.stream(group).mapToLong(i -> ends[i]).max().getAsLong();
            ByteBuffer groupBytes = get(keys, groupStart, groupEnd);
            if (groupBytes == null) {
                return null;
            }
            for (int i : group) {
                // clamp to what was returned, in case the value ends before groupEnd
                ByteBuffer slice = groupBytes.duplicate();
                slice.position((int) Math.min(groupBytes.position() + (starts[i] - groupStart), groupBytes.limit()));
                slice.limit((int) Math.min(slice.position() + (ends[i] - starts[i]), groupBytes.limit()));
                result[i] = slice.slice().order(groupBytes.order());
            }
        }
        return result;
    }

    void set(String[] keys, ByteBuffer bytes);

    void delete(String[] keys);

    @Nonnull
    StoreHandle resolve(String... keys);

    InputStream getInputStream(String[] keys, long start, long end);

    default InputStream getInputStream(String[] keys) {
        return getInputStream(keys, 0, -1);
    }

    /**
     * Gets the size in bytes of the data stored at the given keys.
     *
     * @param keys The keys identifying the data.
     * @return The size in bytes of the data stored at the given keys. -1 if the keys do not exist.
     */
    long getSize(String[] keys);

    /**
     * A store that supports discovery of keys.
     */
    interface ListableStore extends Store {

        /**
         * Recursively lists all keys that contain data (leaf nodes) under the given prefix
         * relative to the prefix.
         * Directory-only entries are excluded.
         *
         * @param prefix The prefix keys to match.
         * @return A stream of key arrays containing data.
         */
        Stream<String[]> list(String[] prefix);

        /**
         * Lists the immediate children (files and virtual directories) under the given prefix.
         * This is useful for UI navigation or browsing the store hierarchy.
         *
         * @param prefix The prefix keys to explore.
         * @return A stream of keys representing one level deeper than the prefix.
         */
        Stream<String> listChildren(String[] prefix);

        /**
         * Lists the immediate children (files and virtual directories) under the store root.
         *
         * @return A stream of keys.
         */
        default Stream<String> listChildren() {
            return listChildren(new String[]{});
        }

        /**
         * Lists all data-bearing keys in the entire store.
         *
         * @return A stream of key arrays containing data.
         */
        default Stream<String[]> list() {
            return list(new String[]{});
        }
    }
}
