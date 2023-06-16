package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.Selector;

import java.nio.ByteBuffer;

public class NoneHandle extends ValueHandle{
    @Override
    public ValueHandle get(Selector selector) {
        return this;
    }

    @Override
    public void set(Selector selector, ValueHandle value) {
    }

    @Override
    public ByteBuffer toBytes() {
        return null;
    }
}
