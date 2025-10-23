package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;

public class AbstractNode implements Node {

    @Nonnull
    public final StoreHandle storeHandle;

    protected AbstractNode(@Nonnull StoreHandle storeHandle) {
        this.storeHandle = storeHandle;
    }
}
