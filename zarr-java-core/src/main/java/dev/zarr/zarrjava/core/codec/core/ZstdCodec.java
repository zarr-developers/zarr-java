package dev.zarr.zarrjava.core.codec.core;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;
import dev.zarr.zarrjava.utils.Utils;

import java.nio.ByteBuffer;

public abstract class ZstdCodec extends BytesBytesCodec {

    @Override
    public ByteBuffer decode(ByteBuffer compressedBytes) throws ZarrException {
        byte[] compressedArray = Utils.toArray(compressedBytes);
        long originalSize = Zstd.getFrameContentSize(compressedArray);
        if (originalSize < 0) {
            throw new ZarrException("Failed to get decompressed zstd size.");
        }
        byte[] decompressed = Zstd.decompress(compressedArray, (int) originalSize);
        return ByteBuffer.wrap(decompressed);
    }

    protected ByteBuffer encodeInternal(int level, boolean checksum, ByteBuffer chunkBytes)
            throws ZarrException {
        byte[] arr = Utils.toArray(chunkBytes);
        byte[] compressed;
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            ctx.setLevel(level);
            ctx.setChecksum(checksum);
            compressed = ctx.compress(arr);
        }
        return ByteBuffer.wrap(compressed);
    }
}
