package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;

public class EndianCodec extends Codec {
    public final class Configuration {
        public String endian = "little";
    }

    public final String name = "endian";
    public Configuration configuration;

    @Override
    public ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }

    @Override
    public ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }
}

