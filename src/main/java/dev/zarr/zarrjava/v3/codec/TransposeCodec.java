package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.v3.ArrayMetadata;

public class TransposeCodec extends Codec {
    public final class Configuration {
        public String order = "C";
    }

    public final String name = "transpose";
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
