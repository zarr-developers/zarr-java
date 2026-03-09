package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.codec.Codec;

import javax.annotation.Nonnull;

public class ZstdCodec extends dev.zarr.zarrjava.core.codec.core.ZstdCodec implements Codec {

    @JsonIgnore
    public final String name = "zstd";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZstdCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected int getLevel() {
        return configuration.level;
    }

    @Override
    protected boolean getChecksum() {
        return configuration.checksum;
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for Zstd codec.");
    }

    public static final class Configuration {

        public final int level;
        public final boolean checksum;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "level", defaultValue = "5") int level,
                             @JsonProperty(value = "checksum", defaultValue = "true") boolean checksum)
                throws ZarrException {
            if (level < -131072 || level > 22) {
                throw new ZarrException("'level' needs to be between -131072 and 22.");
            }
            this.level = level;
            this.checksum = checksum;
        }
    }
}


