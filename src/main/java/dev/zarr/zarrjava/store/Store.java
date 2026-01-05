package dev.zarr.zarrjava.store;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    interface ListableStore extends Store {

      /**
   * Lists all keys in the store that match the given prefix keys. Keys are represented as arrays of strings,
   * where each string is a segment of the key path.
   * Keys that are exactly equal to the prefix are not included in the results.
   * Keys that do not contain data (i.e. "directories") are included in the results.
   *
   * @param keys The prefix keys to match.
   * @return A stream of key arrays that match the given prefix. Prefixed keys are not included in the results.
   */
    Stream<String[]> list(String[] keys);

    default Stream<String[]> list() {
        return list(new String[]{});
    }
  }

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
}
