package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class ZstdCodec extends Codec implements BytesBytesCodec {

    public final String name = "zstd";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ZstdCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ByteBuffer decode(ByteBuffer compressedBytes) throws ZarrException {
        byte[] compressedArray = compressedBytes.array();

        long originalSize = Zstd.decompressedSize(compressedArray);
        if (originalSize == 0) {
            throw new ZarrException("Failed to get decompressed size");
        }

        byte[] decompressed = Zstd.decompress(compressedArray, (int) originalSize);
        return ByteBuffer.wrap(decompressed);
    }

    @Override
    public ByteBuffer encode(ByteBuffer chunkBytes) throws ZarrException {
        byte[] arr = chunkBytes.array();
        byte[] compressed;
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            ctx.setLevel(configuration.level);
            ctx.setChecksum(configuration.checksum);
            compressed = ctx.compress(arr);
        }
        return ByteBuffer.wrap(compressed);
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


