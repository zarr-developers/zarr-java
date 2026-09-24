package dev.zarr.zarrjava.v2.chunkkeyencoding;

import dev.zarr.zarrjava.core.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class V2ChunkKeyEncoding implements ChunkKeyEncoding {

    public final String name = "v2";
    @Nonnull
    public final Separator separator;

    public V2ChunkKeyEncoding(
            @Nonnull Separator separator
    ) {
        this.separator = separator;
    }

    @Override
    public String[] encodeChunkKey(long[] chunkCoords) {
        Stream<String> keys = Arrays.stream(chunkCoords)
                .mapToObj(Long::toString);
        if (separator == Separator.SLASH) {
            return keys.toArray(String[]::new);
        }
        return new String[]{keys.collect(Collectors.joining(this.separator.getValue()))};
    }
}

