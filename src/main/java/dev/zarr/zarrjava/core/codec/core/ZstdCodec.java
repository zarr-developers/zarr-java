package dev.zarr.zarrjava.core.codec.core;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.BytesBytesCodec;

import java.nio.ByteBuffer;

public abstract class ZstdCodec extends BytesBytesCodec {

    protected abstract int getLevel();

    protected abstract boolean getChecksum();

    @Override
    public ByteBuffer decode(ByteBuffer compressedBytes) throws ZarrException {
        byte[] compressedArray = compressedBytes.array();

        long originalSize = Zstd.getFrameContentSize(compressedArray);
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
            ctx.setLevel(getLevel());
            ctx.setChecksum(getChecksum());
            compressed = ctx.compress(arr);
        }
        return ByteBuffer.wrap(compressed);
    }
}
