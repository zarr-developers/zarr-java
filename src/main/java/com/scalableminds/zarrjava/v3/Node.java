package com.scalableminds.zarrjava.v3;

import com.scalableminds.zarrjava.store.Store;

import javax.annotation.Nonnull;

public class Node {
    @Nonnull
    public final Store store;
    @Nonnull
    public final String path;

    protected Node(@Nonnull Store store, @Nonnull String path) {
        this.store = store;
        this.path = path;
    }
}
