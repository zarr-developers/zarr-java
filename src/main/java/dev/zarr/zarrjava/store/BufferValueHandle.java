package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.indexing.Selector;

public class BufferValueHandle extends ValueHandle {

    byte[] bytes;
    public BufferValueHandle(byte[] bytes) {
        this.bytes=bytes;
    }

    @Override
    public ValueHandle get(Selector selector) {
        return null;
    }

    @Override
    public void set(Selector selector, ValueHandle value) {

    }

    @Override
    public byte[] toBytes() {
        return bytes;
    }
}
