package com.scalableminds.zarrjava.store;

import java.nio.ByteBuffer;
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

    String[] list(String[] keys);
  }
}
