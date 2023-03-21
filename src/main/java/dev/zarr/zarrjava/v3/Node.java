package dev.zarr.zarrjava.v3;

import dev.zarr.zarrjava.store.Store;

public class Node {
    public Store store;
    public String path;

    protected Node(Store store, String path) {
        this.store = store;
        this.path = path;
    }
}
