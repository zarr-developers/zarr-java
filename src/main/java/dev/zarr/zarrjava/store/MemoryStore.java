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
        for (String key : keys) {
            if (key.startsWith("/")) {
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

    @Override
    public Stream<String[]> list(String[] prefix) {
        List<String> prefixList = resolveKeys(prefix);
        int prefixSize = prefixList.size();

        return map.keySet().stream()
                .filter(key -> key.size() >= prefixSize && key.subList(0, prefixSize).equals(prefixList))
                .map(key -> key.subList(prefixSize, key.size()).toArray(new String[0]));
    }

    @Override
    public Stream<String[]> listChildren(String[] prefix) {
        List<String> prefixList = resolveKeys(prefix);
        int prefixSize = prefixList.size();

        return map.keySet().stream()
                .filter(key -> key.size() > prefixSize && key.subList(0, prefixSize).equals(prefixList))
                // Identify the immediate child segment
                // e.g. if prefix is [a], and key is [a, b, c], the child is [a, b]
                .map(key -> {
                    List<String> childPath = new ArrayList<>(prefixList);
                    childPath.add(key.get(prefixSize));
                    return childPath;
                })
                .distinct()
                .map(list -> list.toArray(new String[0]));
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
        return new java.io.ByteArrayInputStream(bytes, (int) start, (int) (end - start));
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
