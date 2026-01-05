package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.utils.Utils;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StoreHandle {

  @Nonnull
  public final Store store;
  @Nonnull
  public final String[] keys;

  public StoreHandle(@Nonnull Store store, @Nonnull String... keys) {
    this.store = store;
    this.keys = keys;
  }

  @Nullable
  public ByteBuffer read() {
    return store.get(keys);
  }

  @Nonnull
  public ByteBuffer readNonNull() throws NoSuchFileException {
    ByteBuffer bytes = read();
    if (bytes == null) {
      throw new NoSuchFileException(this.toString());
    }
    return bytes;
  }

  @Nullable
  public ByteBuffer read(long start) {
    return store.get(keys, start);
  }

  @Nullable
  public ByteBuffer read(long start, long end) {
    return store.get(keys, start, end);
  }

  public InputStream getInputStream(int start, int end) {
    return store.getInputStream(keys, start, end);
  }

  public InputStream getInputStream() {
    return store.getInputStream(keys);
  }

  public void set(ByteBuffer bytes) {
    store.set(keys, bytes);
  }

  public void delete() {
    store.delete(keys);
  }

  public boolean exists() {
    return store.exists(keys);
  }

  public Stream<String[]> list() {
    if (!(store instanceof Store.ListableStore)) {
      throw new UnsupportedOperationException("The underlying store does not support listing.");
    }
    return ((Store.ListableStore) store).list(keys);
  }

  public long getSize() {
    return store.getSize(keys);
  }

  @Override
  public String toString() {
    return store + "/" + String.join("/", keys);
  }

  public StoreHandle resolve(String... subKeys) {
    return new StoreHandle(store, Utils.concatArrays(keys, subKeys));
  }

  public Path toPath() {
    if (!(store instanceof FilesystemStore)) {
      throw new UnsupportedOperationException("The underlying store is not a filesystem store.");
    }
    return ((FilesystemStore) store).resolveKeys(keys);
  }
}
