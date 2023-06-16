package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.Selector;

import java.nio.ByteBuffer;

public abstract class ValueHandle {
    public abstract ValueHandle get(Selector selector);

    public abstract void set(Selector selector, ValueHandle value);

    public abstract ByteBuffer toBytes();
}
