package com.scalableminds.zarrjava.store;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Store {
    @Nullable
    ByteBuffer get(String key);

    @Nullable
    ByteBuffer get(String key, long start);

    @Nullable
    ByteBuffer get(String key, long start, long end);

    void set(String key, ByteBuffer bytes);

    void delete(String key);

    interface ListableStore extends Store {
        Iterator<String> list(String key);
    }
}
