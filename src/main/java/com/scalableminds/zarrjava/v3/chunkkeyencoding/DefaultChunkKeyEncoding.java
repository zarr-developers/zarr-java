package com.scalableminds.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultChunkKeyEncoding extends ChunkKeyEncoding {
    public final String name = "default";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator
    public DefaultChunkKeyEncoding(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public long[] decodeChunkKey(String chunkKey) {
        if (chunkKey.equals("c")) {
            return new long[0];
        }
        String suffix = chunkKey.substring(1);
        return Arrays.stream(suffix.split(this.configuration.separator.getValue())).mapToLong(
                Long::parseLong).toArray();
    }

    @Override
    public String encodeChunkKey(long[] chunkCoords) {
        return Stream.concat(Stream.of("c"), Arrays.stream(chunkCoords).mapToObj(Long::toString)).collect(
                Collectors.joining(this.configuration.separator.getValue()));
    }

    public static final class Configuration {
        @Nonnull
        public final Separator separator;

        @JsonCreator
        public Configuration(@Nonnull @JsonProperty(value = "separator", defaultValue = "/") Separator separator) {
            this.separator = separator;
        }
    }
}
