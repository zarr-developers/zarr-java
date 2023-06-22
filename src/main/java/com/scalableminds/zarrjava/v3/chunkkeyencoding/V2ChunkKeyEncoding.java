package com.scalableminds.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;

public class V2ChunkKeyEncoding extends ChunkKeyEncoding {
    public final String name = "v2";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator
    public V2ChunkKeyEncoding(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public long[] decodeChunkKey(String chunkKey) {
        return Arrays.stream(chunkKey.split(this.configuration.separator.getValue())).mapToLong(
                Long::parseLong).toArray();
    }

    @Override
    public String encodeChunkKey(long[] chunkCoords) {
        return Arrays.stream(chunkCoords).mapToObj(Long::toString).collect(
                Collectors.joining(this.configuration.separator.getValue()));
    }

    public static final class Configuration {
        public final Separator separator;

        @JsonCreator
        public Configuration(@Nonnull @JsonProperty(value = "separator", defaultValue = ".") Separator separator) {
            this.separator = separator;
        }
    }
}

