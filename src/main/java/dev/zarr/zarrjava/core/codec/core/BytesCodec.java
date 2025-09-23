package dev.zarr.zarrjava.core.codec.core;

import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class BytesCodec extends ArrayBytesCodec {
    protected abstract ByteOrder getByteOrder();

    @Override
    public Array decode(ByteBuffer chunkBytes) {
        chunkBytes.order(getByteOrder());
        return Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape,
            chunkBytes);
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
