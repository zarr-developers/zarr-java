package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.Selector;

import java.nio.ByteBuffer;

public class BufferValueHandle extends ValueHandle {

    ByteBuffer bytes;

    public BufferValueHandle(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public BufferValueHandle(byte[] bytes) {
        this.bytes = ByteBuffer.wrap(bytes);
    }

    @Override
    public ValueHandle get(Selector selector) {
        return null;
    }

    @Override
    public void set(Selector selector, ValueHandle value) {

    }

    @Override
    public ByteBuffer toBytes() {
        return bytes;
    }
}
