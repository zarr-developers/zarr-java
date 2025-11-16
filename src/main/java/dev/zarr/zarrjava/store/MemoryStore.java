package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class MemoryStore implements Store, Store.ListableStore {
  private final Map<String[], byte[]> map = new HashMap<>();

  @Override
  public boolean exists(String[] keys) {
    return map.containsKey(keys);
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
    if(end>Integer.MAX_VALUE) throw new RuntimeException("TODO");
    if(!map.containsKey(keys)) return null; //TODO: necessary?
    if (end < 0) end = map.get(keys).length - end;
    return ByteBuffer.wrap(map.get(keys), (int) start, (int) end);
  }


  @Override
  public void set(String[] keys, ByteBuffer bytes) {
    map.put(keys, bytes.array());
  }

  @Override
  public void delete(String[] keys) {
    map.remove(keys);
  }

  public Stream<String> list(String[] keys) {
    Set<String> allKeys = new HashSet<>();
    for(String[] k: map.keySet()){
      if (!equalFirstKeys(k, keys, keys.length));
      String key = "";
        for (String s : k) {
            key += s + "/";
            allKeys.add(key);
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

  private boolean equalFirstKeys(String[] k1, String[] k2, int n){
    if (k1.length < n || k2.length < n) return false;
    for (int i = 0; i < n; i++) {
      if(!k1[i].equals(k2[i]))
        return false;
    }
    return true;
  }

}
