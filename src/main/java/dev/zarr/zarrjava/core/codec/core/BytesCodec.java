package dev.zarr.zarrjava.core.codec.core;

import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import ucar.ma2.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class BytesCodec extends ArrayBytesCodec {
    protected abstract ByteOrder getByteOrder() throws ZarrException;

    @Override
    public Array decode(ByteBuffer chunkBytes) throws ZarrException {
        ByteOrder order = ByteOrder.BIG_ENDIAN; // Default for 1-byte types
        if (arrayMetadata.dataType.getByteCount() > 1)
            order = getByteOrder();
        chunkBytes.order(order);
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
    public ByteBuffer encode(Array chunkArray) throws ZarrException {
        ByteOrder order = ByteOrder.BIG_ENDIAN; // Default for 1-byte types
        if (arrayMetadata.dataType.getByteCount() > 1)
            order = getByteOrder();

        // Boolean
        if (chunkArray instanceof ArrayBoolean) {
            boolean[] data = (boolean[]) chunkArray.copyTo1DJavaArray();
            ByteBuffer bb = ByteBuffer.allocate(data.length);
            for (boolean b : data) {
                bb.put((byte) (b ? 1 : 0));
            }
            bb.flip();
            return bb;
        }

        // Float32
        if (chunkArray instanceof ArrayFloat) {
            float[] data = (float[]) chunkArray.copyTo1DJavaArray();
            ByteBuffer bb = ByteBuffer.allocate(data.length * Float.BYTES).order(order);
            for (float f : data) {
                bb.putFloat(f);
            }
            bb.flip();
            return bb;
        }

        // Float64
        if (chunkArray instanceof ArrayDouble) {
            double[] data = (double[]) chunkArray.copyTo1DJavaArray();
            ByteBuffer bb = ByteBuffer.allocate(data.length * Double.BYTES).order(order);
            for (double d : data) {
                bb.putDouble(d);
            }
            bb.flip();
            return bb;
        }

    // All other primitive types (byte, short, int, long, char)
    return chunkArray.getDataAsByteBuffer(order);
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

        public static Endian nativeOrder() {
            return ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? LITTLE : BIG;
        }
    }

}
