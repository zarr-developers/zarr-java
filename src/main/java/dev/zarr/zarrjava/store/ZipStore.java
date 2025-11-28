package dev.zarr.zarrjava.store;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class ZipStore implements Store, Store.ListableStore {
    @Nonnull
    private final Path path;

    public ZipStore(@Nonnull Path path) {
        this.path = path;
    }

    public ZipStore(@Nonnull String path) {
        this.path = Paths.get(path);
    }


    @Override
    public Stream<String> list(String[] keys) {
        return Stream.empty();
    }

    @Override
    public boolean exists(String[] keys) {
        return false;
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        return null;
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        return null;
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {

    }

    @Override
    public void delete(String[] keys) {

    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public String toString() {
        return this.path.toUri().toString().replaceAll("\\/$", "");
    }

}
