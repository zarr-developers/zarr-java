package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.Store;
import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * A {@link MemoryStore} that counts how often it is asked to list or read, so that tests can
 * assert on the number of store operations a group traversal costs.
 */
public final class CountingStore implements Store, Store.ListableStore {

    private final MemoryStore delegate = new MemoryStore();
    public final AtomicInteger listCalls = new AtomicInteger();
    public final AtomicInteger listChildrenCalls = new AtomicInteger();
    public final AtomicInteger readCalls = new AtomicInteger();

    public void resetCounters() {
        listCalls.set(0);
        listChildrenCalls.set(0);
        readCalls.set(0);
    }

    @Override
    public Stream<String[]> list(String[] prefix) {
        listCalls.incrementAndGet();
        return delegate.list(prefix);
    }

    @Override
    public Stream<String> listChildren(String[] prefix) {
        listChildrenCalls.incrementAndGet();
        return delegate.listChildren(prefix);
    }

    @Override
    public boolean exists(String[] keys) {
        readCalls.incrementAndGet();
        return delegate.exists(keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        readCalls.incrementAndGet();
        return delegate.get(keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        readCalls.incrementAndGet();
        return delegate.get(keys, start);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        readCalls.incrementAndGet();
        return delegate.get(keys, start, end);
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        delegate.set(keys, bytes);
    }

    @Override
    public void delete(String[] keys) {
        delegate.delete(keys);
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        readCalls.incrementAndGet();
        return delegate.getInputStream(keys, start, end);
    }

    @Override
    public long getSize(String[] keys) {
        return delegate.getSize(keys);
    }

    @Override
    public String toString() {
        return "<CountingStore>";
    }
}
