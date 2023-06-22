package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

public interface Store {

    Optional<ByteBuffer> get(String key, OpenSlice byteRange);

    void set(String key, ByteBuffer bytes, OpenSlice byteRange);

    void delete(String key);

    interface ListableStore extends Store {
        Iterator<String> list(String key);
    }
}
