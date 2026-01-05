package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class MemoryStore implements Store, Store.ListableStore {
  private final Map<List<String>, byte[]> map = new ConcurrentHashMap<>();

  List<String> resolveKeys(String[] keys) {
    ArrayList<String> resolvedKeys = new ArrayList<>();
    for(String key:keys){
      if(key.startsWith("/")){
        key = key.substring(1);
      }
      resolvedKeys.addAll(Arrays.asList(key.split("/")));
    }
    return resolvedKeys;
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
    if (end > Integer.MAX_VALUE) throw new IllegalArgumentException("End index too large");
    return ByteBuffer.wrap(bytes, (int) start, (int) (end - start));
  }


  @Override
  public void set(String[] keys, ByteBuffer bytes) {
    map.put(resolveKeys(keys), bytes.array());
  }

  @Override
  public void delete(String[] keys) {
    map.remove(resolveKeys(keys));
  }

  public Stream<String[]> list(String[] keys) {
      List<String> prefix = resolveKeys(keys);
      Set<List<String>> allKeys = new HashSet<>();

      for (List<String> k : map.keySet()) {
          if (k.size() <= prefix.size() || ! k.subList(0, prefix.size()).equals(prefix))
              continue;
          for (int i = prefix.size(); i < k.size(); i++) {
              allKeys.add(k.subList(0, i+1));
          }
      }
      return allKeys.stream().map(k -> k.toArray(new String[0]));
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

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        byte[] bytes = map.get(resolveKeys(keys));
        if (bytes == null) return null;
        if (end < 0) end = bytes.length;
        if (end > Integer.MAX_VALUE) throw new IllegalArgumentException("End index too large");
        return new java.io.ByteArrayInputStream(bytes, (int) start, (int)(end - start));
    }

    @Override
    public long getSize(String[] keys) {
        byte[] bytes = map.get(resolveKeys(keys));
        if (bytes == null) {
            return -1;
        }
        return bytes.length;
    }
}
