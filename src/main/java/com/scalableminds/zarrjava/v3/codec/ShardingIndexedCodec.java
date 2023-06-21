package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.store.BufferValueHandle;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.Utils;
import com.scalableminds.zarrjava.v3.codec.Codec.ArrayBytesCodec;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.zip.CRC32;


public class ShardingIndexedCodec extends ArrayBytesCodec {
    public final String name = "sharding_indexed";
    public Configuration configuration;

    long[][] parseShardIndex(ByteBuffer buffer, int count) {
        long[][] index = new long[count][2];
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int oldBufferLimit = buffer.limit();
        int oldBufferPosition = buffer.position();
        buffer.limit(buffer.position() + count * 16);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        int computedCrc32 = (int) crc32.getValue();

        buffer.limit(oldBufferLimit);
        int storedCrc32 = buffer.getInt();

        // assert computedCrc32 == storedCrc32;
        buffer.position(oldBufferPosition);

        for (int i = 0; i < count; i++) {
            index[i][0] = buffer.getLong();
            index[i][1] = buffer.getLong();
        }
        buffer.position(oldBufferPosition);
        return index;
    }

    @Override
    public Array innerDecode(ByteBuffer shardBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        Array outputArray = Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape);

        int ndim = arrayMetadata.ndim();
        int[] chunksPerShard = new int[ndim];
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++)
            chunksPerShard[dimIdx] = arrayMetadata.chunkShape[dimIdx] / configuration.chunkShape[dimIdx];
        int chunkCount = Arrays.stream(chunksPerShard).reduce(1, (r, a) -> r * a);

        int shardIndexByteLength = chunkCount * 16 + 4;
        shardBytes.position(shardBytes.capacity() - shardIndexByteLength);
        long[][] shardIndex = parseShardIndex(shardBytes, chunkCount);

        ArrayMetadata.CoreArrayMetadata shardMetadata =
                new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
                        configuration.chunkShape, arrayMetadata.dataType, arrayMetadata.fillValue);

        int i = 0;
        for (long[] chunkCoords : Indexer.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape)) {
            int chunkByteOffset = (int) shardIndex[i][0];
            int chunkByteLength = (int) shardIndex[i][1];
            Array chunkArray = null;
            Indexer.ChunkProjection chunkProjection =
                    Indexer.computeProjection(chunkCoords, shardMetadata.shape, shardMetadata.chunkShape);
            if (chunkByteOffset != -1 && chunkByteLength != -1) {
                shardBytes.limit(chunkByteOffset + chunkByteLength);
                shardBytes.position(chunkByteOffset);
                ByteBuffer chunkBytes = ByteBuffer.allocate(chunkByteLength).put(shardBytes);
                BufferValueHandle chunkValueHandle = new BufferValueHandle(chunkBytes);

                chunkArray = decodeChunk(chunkValueHandle, shardMetadata);
                System.out.printf("%s %d %d %d %o\n", Arrays.toString(chunkCoords), chunkByteOffset, chunkByteLength,
                        shardBytes.capacity(), chunkArray);
                System.out.flush();
            }
            if (chunkArray == null) {
                chunkArray = shardMetadata.allocateFillValueChunk();
            }
            Indexer.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray, chunkProjection.outOffset,
                    chunkProjection.shape);
            i++;
        }

        return outputArray;
    }

    private Array decodeChunk(ValueHandle chunkValueHandle, ArrayMetadata.CoreArrayMetadata shardMetadata) {
        if (configuration.codecs.isPresent() && configuration.codecs.get().length > 0) {
            Codec[] codecs = configuration.codecs.get();
            for (int i = codecs.length - 1; i >= 0; --i) {
                chunkValueHandle = codecs[i].decode(chunkValueHandle, shardMetadata);
            }
        }
        return chunkValueHandle.toArray(shardMetadata.chunkShape, shardMetadata.dataType);
    }

    @Override
    public ByteBuffer innerEncode(Array shardArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return shardArray.getDataAsByteBuffer();
    }

    public static final class Configuration {
        @JsonProperty("chunk_shape")
        public int[] chunkShape;
        public Optional<Codec[]> codecs;
    }
}
