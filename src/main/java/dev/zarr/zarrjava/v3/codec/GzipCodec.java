package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.v3.ArrayMetadata;

public class GzipCodec extends Codec {
    public final class Configuration {
        public int level = 5;
    }

    public final String name = "gzip";
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
