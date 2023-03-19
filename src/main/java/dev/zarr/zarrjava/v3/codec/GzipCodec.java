package dev.zarr.zarrjava.v3.codec;

public class GzipCodec extends Codec {
    public final class Configuration {
        public int level = 5;
    }

    public final String name = "gzip";
    public Configuration configuration;
}
