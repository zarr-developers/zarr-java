package dev.zarr.zarrjava.interfaces;

import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import ucar.ma2.Array;

public interface ArrayMetadata {
    int ndim();

    int[] chunkShape();

    long[] shape();

    DataType dataType();

    Array allocateFillValueChunk();

    ChunkKeyEncoding chunkKeyEncoding();

    Object parsedFillValue();

}
