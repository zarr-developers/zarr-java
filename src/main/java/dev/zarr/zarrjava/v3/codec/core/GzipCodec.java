package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class GzipCodec extends dev.zarr.zarrjava.core.codec.core.GzipCodec implements Codec {

    @JsonIgnore
    public final String name = "gzip";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GzipCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException {
        return encodeInternal(configuration.level, chunkBytes);
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Gzip codec.");
    }

    public static final class Configuration {

        public final int level;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "level", defaultValue = "5") int level)
                throws ZarrException {
            if (level < 0 || level > 9) {
                throw new ZarrException("'level' needs to be between 0 and 9.");
            }
            this.level = level;
        }
    }
}
