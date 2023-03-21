package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.v3.ArrayMetadata;

public class EndianCodec extends Codec {
    public final class Configuration {
        public String endian = "little";
    }

    public final String name = "endian";
    public Configuration configuration;

    @Override
    public byte[] decode(byte[] chunk, long[][] selection, ArrayMetadata arrayMetadata) {
        return chunk;
    }

    @Override
    public byte[] encode(byte[] chunk, long[][] selection, ArrayMetadata arrayMetadata) {
        return chunk;
    }
}

