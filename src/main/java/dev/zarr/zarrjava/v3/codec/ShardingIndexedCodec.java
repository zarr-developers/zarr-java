package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;

import java.util.Optional;

public class ShardingIndexedCodec extends Codec {
    public final String name = "sharding_indexed";
    public Configuration configuration;

    public final class Configuration {
        @JsonProperty("chunk_shape")
        public long[] chunkShape;
        public Optional<Codec[]> codecs;
    }

    @Override
    public ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }

    @Override
    public ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }
}
