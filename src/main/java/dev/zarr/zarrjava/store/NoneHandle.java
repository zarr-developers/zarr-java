package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.indexing.Selector;

public class NoneHandle extends ValueHandle{
    @Override
    public ValueHandle get(Selector selector) {
        return this;
    }

    @Override
    public void set(Selector selector, ValueHandle value) {
    }

    @Override
    public byte[] toBytes() {
        return null;
    }
}
