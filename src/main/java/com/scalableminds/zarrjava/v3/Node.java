package com.scalableminds.zarrjava.v3;

import com.scalableminds.zarrjava.store.Store;

public class Node {
    public Store store;
    public String path;

    protected Node(Store store, String path) {
        this.store = store;
        this.path = path;
    }
}
