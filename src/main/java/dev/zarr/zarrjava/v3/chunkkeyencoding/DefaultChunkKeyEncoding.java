package dev.zarr.zarrjava.v3.chunkkeyencoding;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultChunkKeyEncoding extends ChunkKeyEncoding {
    @Override
    public long[] decodeChunkKey(String chunkKey) {
        if (chunkKey.equals("c")) {
            return new long[0];
        }
        String suffix = chunkKey.substring(1);
        return Arrays.stream(suffix.split(this.configuration.separator.getValue())).mapToLong(Long::parseLong).toArray();
    }

    @Override
    public String encodeChunkKey(long[] chunkCoords) {
        return Stream.concat(Stream.of("c"), Arrays.stream(chunkCoords).mapToObj(Long::toString)).collect(Collectors.joining(this.configuration.separator.getValue()));
    }

    public final class Configuration {
        public Separator separator = Separator.SLASH;
    }

    public final String name = "default";
    public Configuration configuration;
}
