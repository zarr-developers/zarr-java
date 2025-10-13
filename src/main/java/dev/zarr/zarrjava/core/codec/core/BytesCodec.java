package dev.zarr.zarrjava.core.codec.core;

import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.Index;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class BytesCodec extends ArrayBytesCodec {
    protected abstract ByteOrder getByteOrder();

    @Override
public Array decode(ByteBuffer chunkBytes) {
    chunkBytes.order(getByteOrder());
    DataType dtype = arrayMetadata.dataType.getMA2DataType();
    int[] shape = arrayMetadata.chunkShape;

    // Array.factory does not support boolean arrays directly from ByteBuffer
    if (dtype == DataType.BOOLEAN) {
        int size = chunkBytes.remaining();
        boolean[] bools = new boolean[size];
        for (int i = 0; i < size; i++) {
            bools[i] = chunkBytes.get(i) != 0;
        }

        Index index = Index.factory(shape);
        return Array.factory(DataType.BOOLEAN, index, bools);
    }

    return Array.factory(dtype, shape, chunkBytes);
}
    @Override
    public ByteBuffer encode(Array chunkArray) {
        return chunkArray.getDataAsByteBuffer(getByteOrder());
    }

    public enum Endian {
        LITTLE("little"),
        BIG("big");
        private final String endian;

        Endian(String endian) {
            this.endian = endian;
        }

        @JsonValue
        public String getValue() {
            return endian;
        }

        public ByteOrder getByteOrder() {
            switch (this) {
                case LITTLE:
                    return ByteOrder.LITTLE_ENDIAN;
                case BIG:
                    return ByteOrder.BIG_ENDIAN;
                default:
                    throw new RuntimeException("Unreachable");
            }
        }
    }

}
