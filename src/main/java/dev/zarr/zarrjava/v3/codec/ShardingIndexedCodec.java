package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class ShardingIndexedCodec extends Codec {
    public final class Configuration {
        @JsonProperty("chunk_shape")
        public long[] chunkShape;
        public Optional<Codec[]> codecs;
    }

    public final String name = "sharding_indexed";
    public Configuration configuration;
}
