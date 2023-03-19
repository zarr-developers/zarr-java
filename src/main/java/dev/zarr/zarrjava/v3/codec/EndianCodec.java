package dev.zarr.zarrjava.v3.codec;

public class EndianCodec extends Codec {
    public final class Configuration {
        public String endian = "little";
    }

    public final String name = "endian";
    public Configuration configuration;
}
