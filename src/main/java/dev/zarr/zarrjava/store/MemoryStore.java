package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class MemoryStore implements Store, Store.ListableStore {
  private final Map<String, byte[]> map = new HashMap<>();
  Separator separator;

  public MemoryStore(Separator separator){
    this.separator = separator;
  }

  public MemoryStore(){
    this(Separator.SLASH);
  }

  String resolveKeys(String[] keys) {
    return String.join(separator.getValue(), keys);
  }

  @Override
  public boolean exists(String[] keys) {
    return map.containsKey(resolveKeys(keys));
  }

  @Nullable
  @Override
  public ByteBuffer get(String[] keys) {
      return get(keys, 0);
  }

  @Nullable
  @Override
  public ByteBuffer get(String[] keys, long start) {
    return get(keys, start, -1);
  }

  @Nullable
  @Override
  public ByteBuffer get(String[] keys, long start, long end) {
    byte[] bytes = map.get(resolveKeys(keys));
    if (bytes == null) return null;
    if (end < 0) end = bytes.length;
    if (end > Integer.MAX_VALUE) throw new RuntimeException("TODO"); //TODO
    return ByteBuffer.wrap(bytes, (int) start, (int) end);
  }


  @Override
  public void set(String[] keys, ByteBuffer bytes) {
    map.put(resolveKeys(keys), bytes.array());
  }

  @Override
  public void delete(String[] keys) {
    map.remove(resolveKeys(keys));
  }

  public Stream<String> list(String[] keys) {
    String prefix = resolveKeys(keys);
    Set<String> allKeys = new HashSet<>();

    for (String k : map.keySet()) {
      if (!k.startsWith(prefix)) continue;
      String current = "";
      for (String s : k.split(separator.getValue())) {
        current += s;
        allKeys.add(current);
        current += separator.getValue();
      }
    }
    return allKeys.stream();
  }

  @Nonnull
  @Override
  public StoreHandle resolve(String... keys) {
    return new StoreHandle(this, keys);
  }

  @Override
  public String toString() {
    return String.format("<MemoryStore {%s}>", hashCode());
  }
}
