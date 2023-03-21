package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.v3.ArrayMetadata;

public class BloscCodec extends Codec {
    public final class Configuration {
        public String cname = "zstd";
        public int clevel = 5;
        public int shuffle = 0;
        public int blocksize = 0;
    }

    public final String name = "blosc";
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
