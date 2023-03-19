package dev.zarr.zarrjava.v3.codec;

public class TransposeCodec extends Codec{
    public final class Configuration {
        public String order = "C";
    }

    public final String name = "transpose";
    public Configuration configuration;
}
