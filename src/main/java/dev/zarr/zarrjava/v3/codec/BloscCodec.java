package dev.zarr.zarrjava.v3.codec;

public class BloscCodec extends Codec {
    public final class Configuration {
        public String cname = "zstd";
        public int clevel = 5;
        public int shuffle = 0;
        public int blocksize = 0;
    }

    public final String name = "blosc";
    public Configuration configuration;
}
