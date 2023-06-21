package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;
import com.scalableminds.zarrjava.v3.DataType;
import ucar.ma2.Array;

import java.nio.ByteBuffer;

public class NoneHandle extends ValueHandle {
    @Override
    public ValueHandle get(OpenSlice slice) {
        return this;
    }

    @Override
    public void set(OpenSlice slice, ValueHandle value) {
    }

    @Override
    public ByteBuffer toBytes() {
        return null;
    }

    @Override
    public Array toArray() {
        return null;
    }

    @Override
    public Array toArray(int[] shape, DataType dataType) {
        return null;
    }
}
