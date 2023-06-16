package com.scalableminds.zarrjava.v3;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class Utils {

    public static ByteBuffer allocateNative(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.nativeOrder());
    }
    public static ByteBuffer makeByteBuffer(int capacity, Function<ByteBuffer, ByteBuffer> func) {
        ByteBuffer buf = allocateNative(capacity);
        buf = func.apply(buf);
        return (ByteBuffer) buf.rewind();
    }
}
