package dev.zarr.zarrjava.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentMemoryStore extends MemoryStore {
  @Override
  protected Map<List<String>, byte[]> map() {
    return new ConcurrentHashMap<>();
  }

  @Override
  public String toString() {
    return String.format("<ConcurrentMemoryStore {%s}>", hashCode());
  }
}
