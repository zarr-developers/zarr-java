package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;

public class GzipCodec extends Codec {
    public final class Configuration {
        public int level = 5;
    }

    public final String name = "gzip";
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
