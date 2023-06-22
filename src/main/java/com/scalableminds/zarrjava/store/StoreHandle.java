package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StoreHandle {
    @Nonnull
    final Store store;
    @Nonnull
    final String key;

    public StoreHandle(@Nonnull Store store, @Nonnull String key) {
        this.store = store;
        this.key = key;
    }

    @Nullable
    public ByteBuffer read() {
        return store.get(key, null).orElse(null);
    }
    @Nullable
    public ByteBuffer read(long start) {
        return store.get(key, new OpenSlice(start, null)).orElse(null);
    }

    public void set(OpenSlice slice, ByteBuffer bytes) {

    }

    @Override
    public ByteBuffer toBytes() {
        return store.get(key, null).orElse(null);
    }
}
