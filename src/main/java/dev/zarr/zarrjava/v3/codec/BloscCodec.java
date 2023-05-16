package dev.zarr.zarrjava.v3.codec;

import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.BufferValueHandle;
import dev.zarr.zarrjava.store.NoneHandle;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import org.blosc.JBlosc;

import java.nio.ByteBuffer;

public class BloscCodec extends Codec {
    public final class Configuration {
        public String cname = "zstd";
        public int clevel = 5;
        public int shuffle = 0;
        public int blocksize = 0;
    }

    public final String name = "blosc";
    public Configuration configuration;

    @Override
    public ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        if (chunk instanceof NoneHandle) {
            return chunk;
        }
        ByteBuffer chunkBuffer = ByteBuffer.wrap(chunk.toBytes());
        int byteLength = arrayMetadata.chunkByteLength();
        ByteBuffer outBuffer = ByteBuffer.allocate(byteLength);
         JBlosc.decompressCtx(chunkBuffer, outBuffer, byteLength,1);
        byte[] out = new byte[byteLength];
        outBuffer.get(out);
        return new BufferValueHandle(out);
    }

    @Override
    public ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }
}
