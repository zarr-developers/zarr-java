package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.v3.DataType;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ArrayValueHandle extends ValueHandle {

    Array array;

    public ArrayValueHandle(Array array) {
        this.array = array;
    }

    @Override
    public ValueHandle get(OpenSlice slice) {
        return null;
    }

    @Override
    public void set(OpenSlice slice, ValueHandle value) {

    }

    @Override
    public ByteBuffer toBytes() {
        return array.getDataAsByteBuffer(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public Array toArray() {
        return array;
    }

    @Override
    public Array toArray(int[] shape, DataType dataType) {
        return array.reshapeNoCopy(shape);
    }
}
