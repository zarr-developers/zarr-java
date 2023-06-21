package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonValue;
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.Codec.ArrayBytesCodec;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EndianCodec extends ArrayBytesCodec {
    public final String name = "endian";
    public Configuration configuration;

    @Override
    public Array innerDecode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        chunkBytes.order(configuration.endian.getByteOrder());
        return Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape, chunkBytes);
    }

    @Override
    public ByteBuffer innerEncode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return chunkArray.getDataAsByteBuffer(configuration.endian.getByteOrder());
    }

    public enum Endian {
        LITTLE("little"), BIG("big");
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

    public static final class Configuration {
        public EndianCodec.Endian endian = Endian.LITTLE;
    }
}

