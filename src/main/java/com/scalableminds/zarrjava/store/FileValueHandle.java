package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.v3.DataType;
import com.scalableminds.zarrjava.indexing.OpenSlice;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.util.Optional;

public class FileValueHandle extends ValueHandle {
    Store store;
    String key;

    public FileValueHandle(Store store, String key) {
        this.store = store;
        this.key = key;
    }

    @Override
    public ValueHandle get(OpenSlice slice) {
        Optional<BufferValueHandle> valueHandle = store.get(key, slice).map(BufferValueHandle::new);
        if (valueHandle.isPresent()) {
            return valueHandle.get();
        }
        return new NoneHandle();
    }

    @Override
    public void set(OpenSlice slice, ValueHandle value) {

    }

    @Override
    public ByteBuffer toBytes() {
        return store.get(key, null).orElse(null);
    }

    @Override
    public Array toArray() {
        return null;
    }

    @Override
    public Array toArray(int[] shape, DataType dataType) {
        ByteBuffer bytes = toBytes();
        if (bytes != null) return Array.factory(dataType.getMA2DataType(), shape, bytes);
        return null;
    }
}
