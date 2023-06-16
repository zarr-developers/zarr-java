package com.scalableminds.zarrjava.v3.chunkkeyencoding;

import java.util.Arrays;
import java.util.stream.Collectors;

public class V2ChunkKeyEncoding extends ChunkKeyEncoding {
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

    public final class Configuration {
        public Separator separator = Separator.DOT;
    }

    public final String name = "v2";
    public Configuration configuration;
}

