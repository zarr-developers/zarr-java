package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v2.ArrayMetadata;
import dev.zarr.zarrjava.v2.codec.Codec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// TODO might move some of this to core
public class BytesCodec extends dev.zarr.zarrjava.core.codec.core.BytesCodec implements Codec {
    @Nonnull
    public final Endian endian;

    public final String name = "bytes";

    @JsonCreator
    public BytesCodec(
        @JsonProperty(value = "endian", defaultValue = "little") Endian endian) {
        this.endian = endian;
    }

    public Array decode(ByteBuffer chunkBytes) {
        chunkBytes.order(endian.getByteOrder());
        return Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape,
            chunkBytes);
    }

    @Override
    public ByteBuffer encode(Array chunkArray) {
        return chunkArray.getDataAsByteBuffer(endian.getByteOrder());
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

