package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.v3.DataType;
import ucar.ma2.Array;

import java.nio.ByteBuffer;

public abstract class ValueHandle {
    public abstract ValueHandle get(OpenSlice slice);

    public abstract void set(OpenSlice slice, ValueHandle value);

    public abstract ByteBuffer toBytes();

    public abstract Array toArray();
    public abstract Array toArray(int[] shape, DataType dataType);
}
