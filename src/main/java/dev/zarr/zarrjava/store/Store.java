package dev.zarr.zarrjava.store;

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

    Stream<String> list(String[] keys);
  }
}
