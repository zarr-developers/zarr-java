package dev.zarr.zarrjava.core;

public interface DataType {
    ucar.ma2.DataType getMA2DataType();

    int getByteCount();
}
