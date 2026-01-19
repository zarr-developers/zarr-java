package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface Store {

    boolean exists(String[] keys);

    @Nullable
    ByteBuffer get(String[] keys);

    @Nullable
    ByteBuffer get(String[] keys, long start);

    @Nullable
    ByteBuffer get(String[] keys, long start, long end);

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
         * @return A stream of full key arrays containing data.
         */
        Stream<String[]> list(String[] prefix);

        /**
         * Lists the immediate children (files and virtual directories) under the given prefix.
         * This is useful for UI navigation or browsing the store hierarchy.
         *
         * @param prefix The prefix keys to explore.
         * @return A stream of key arrays representing one level deeper than the prefix.
         */
        Stream<String[]> listChildren(String[] prefix);

        /**
         * Lists all data-bearing keys in the entire store.
         */
        default Stream<String[]> list() {
            return list(new String[]{});
        }
    }
}
