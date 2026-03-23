package dev.zarr.zarrjava.v2.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;
import dev.zarr.zarrjava.v2.codec.Codec;

import java.nio.ByteBuffer;

public class ZstdCodec extends dev.zarr.zarrjava.core.codec.core.ZstdCodec implements Codec {

    public static final int DEFAULT_LEVEL = 0;
    public static final boolean DEFAULT_CHECKSUM = false;
    @JsonIgnore
    public final String id = "zstd";
    public final int level;
    public final boolean checksum;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZstdCodec(
            @JsonProperty(value = "level", defaultValue = "" + DEFAULT_LEVEL) int level,
            @JsonProperty(value = "checksum", defaultValue = "" + DEFAULT_CHECKSUM) boolean checksum) throws ZarrException {
        if (level < -131072 || level > 22) {
            throw new ZarrException("'level' needs to be between -131072 and 22.");
        }
        this.level = level;
        this.checksum = checksum;
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException {
        return encodeInternal(this.level, this.checksum, chunkBytes);
    }

    @Override
    public Codec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return this;
    }
}
