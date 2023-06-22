package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.Utils;
import com.scalableminds.zarrjava.v3.codec.ArrayBytesCodec;
import com.scalableminds.zarrjava.v3.codec.Codec;
import com.scalableminds.zarrjava.v3.codec.CodecPipeline;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32;


public class ShardingIndexedCodec implements ArrayBytesCodec, ArrayBytesCodec.WithPartialEncode, ArrayBytesCodec.WithPartialDecode {
    public final String name = "sharding_indexed";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ShardingIndexedCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration) {
        this.configuration = configuration;
    }

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
    public Array decode(ByteBuffer shardBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        Array outputArray = Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape);

        int ndim = arrayMetadata.ndim();
        int[] chunksPerShard = new int[ndim];
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++)
            chunksPerShard[dimIdx] = arrayMetadata.chunkShape[dimIdx] / configuration.chunkShape[dimIdx];
        int chunkCount = Arrays.stream(chunksPerShard).reduce(1, (r, a) -> r * a);

        int shardIndexByteLength = chunkCount * 16 + 4;
        shardBytes.position(shardBytes.capacity() - shardIndexByteLength);
        ByteBuffer shardIndexBytes = shardBytes.slice();
        long[][] shardIndex = parseShardIndex(shardIndexBytes, chunkCount);

        shardBytes.position(0);

        ArrayMetadata.CoreArrayMetadata shardMetadata =
                new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
                        configuration.chunkShape, arrayMetadata.dataType, arrayMetadata.fillValue);

        Arrays.stream(Indexer.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape)).forEach(
                chunkCoords -> {
                    int i = (int) Indexer.cOrderIndex(chunkCoords, Utils.toLongArray(chunksPerShard));
                    ByteBuffer shardBytesSlice = shardBytes.slice();

                    int chunkByteOffset = (int) shardIndex[i][0];
                    int chunkByteLength = (int) shardIndex[i][1];
                    Array chunkArray = null;
                    Indexer.ChunkProjection chunkProjection =
                            Indexer.computeProjection(chunkCoords, shardMetadata.shape, shardMetadata.chunkShape);
                    if (chunkByteOffset != -1 && chunkByteLength != -1) {
                        shardBytesSlice.limit(chunkByteOffset + chunkByteLength);
                        shardBytesSlice.position(chunkByteOffset);
                        ByteBuffer chunkBytes = ByteBuffer.allocate(chunkByteLength).put(shardBytesSlice);

                        chunkArray = new CodecPipeline(configuration.codecs).decode(chunkBytes, shardMetadata);
                    }
                    if (chunkArray == null) {
                        chunkArray = shardMetadata.allocateFillValueChunk();
                    }
                    Indexer.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray, chunkProjection.outOffset,
                            chunkProjection.shape);
                });

        return outputArray;
    }


    @Override
    public ByteBuffer encode(Array shardArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        return shardArray.getDataAsByteBuffer();
    }

    @Override
    public ByteBuffer encodePartial(Array chunkArray, long[] offset, int[] shape, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        // TODO
        return null;
    }

    @Override
    public Array partialDecode(ByteBuffer chunkBytes, long[] offset, int[] shape, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        // TODO
        return null;
    }

    public static final class Configuration {
        @JsonProperty("chunk_shape")
        public final int[] chunkShape;
        @Nullable
        public final Codec[] codecs;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(
                @JsonProperty(value = "chunk_shape", required = true) int[] chunkShape,
                @Nullable @JsonProperty("codecs") Codec[] codecs) {
            this.chunkShape = chunkShape;
            this.codecs = codecs;
        }
    }
}
