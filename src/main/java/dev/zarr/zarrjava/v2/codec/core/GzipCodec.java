package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.v2.codec.Codec;

import java.nio.ByteBuffer;

public class GzipCodec extends dev.zarr.zarrjava.core.codec.core.GzipCodec implements Codec {

    public static final int DEFAULT_LEVEL = 1;
    @JsonIgnore
    public final String id = "gzip";
    public final int level;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GzipCodec(
            @JsonProperty(value = "level", defaultValue = "" + DEFAULT_LEVEL) int level)
            throws ZarrException {
        if (level < 0 || level > 9) {
            throw new ZarrException("'level' needs to be between 0 and 9.");
        }
        this.level = level;
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException {
        return encodeInternal(this.level, chunkBytes);
    }

    @Override
    public Codec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return this;
    }
}
