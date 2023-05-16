package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.indexing.Selector;

public abstract class ValueHandle {
    public abstract ValueHandle get(Selector selector);

    public abstract void set(Selector selector, ValueHandle value);

    public abstract byte[] toBytes();
}
