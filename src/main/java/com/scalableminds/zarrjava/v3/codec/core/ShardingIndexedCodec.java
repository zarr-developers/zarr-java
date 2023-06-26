package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.indexing.CRC32C;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.indexing.MultiArrayUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
        final long[][] index = new long[count][2];
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.limit(buffer.position() + count * 16);

        final CRC32C crc32c = new CRC32C();
        crc32c.update(buffer);
        int computedCrc32c = (int) crc32c.getValue();

        buffer.limit(buffer.capacity());
        int storedCrc32c = buffer.getInt();

        // assert computedCrc32c == storedCrc32c;
        buffer.rewind();

        for (int i = 0; i < count; i++) {
            index[i][0] = buffer.getLong();
            index[i][1] = buffer.getLong();
        }
        return index;
    }

    ByteBuffer writeShardIndex(long[][] shardIndex) {
        ByteBuffer buffer = Utils.makeByteBuffer(shardIndex.length * 16 + 4, b -> {
            for (final long[] shardEntry : shardIndex) {
                b.putLong(shardEntry[0]);
                b.putLong(shardEntry[1]);
            }
            return b;
        });
        buffer.limit(shardIndex.length * 16);

        final CRC32C crc32c = new CRC32C();
        crc32c.update(buffer);
        int computedCrc32c = (int) crc32c.getValue();
        buffer.limit(buffer.capacity());
        buffer.putInt(computedCrc32c);
        buffer.rewind();
        return buffer;
    }

    public int[] getChunksPerShard(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        final int ndim = arrayMetadata.ndim();
        final int[] chunksPerShard = new int[ndim];
        for (int dimIdx = 0; dimIdx < ndim; dimIdx++)
            chunksPerShard[dimIdx] = arrayMetadata.chunkShape[dimIdx] / configuration.chunkShape[dimIdx];
        return chunksPerShard;
    }

    @Override
    public Array decode(ByteBuffer shardBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        final Array outputArray = Array.factory(arrayMetadata.dataType.getMA2DataType(), arrayMetadata.chunkShape);

        final int[] chunksPerShard = getChunksPerShard(arrayMetadata);
        final int chunkCount = Arrays.stream(chunksPerShard).reduce(1, (r, a) -> r * a);

        final int shardIndexByteLength = chunkCount * 16 + 4;
        shardBytes.position(shardBytes.capacity() - shardIndexByteLength);
        final ByteBuffer shardIndexBytes = shardBytes.slice();
        final long[][] shardIndex = parseShardIndex(shardIndexBytes, chunkCount);

        shardBytes.position(0);

        final ArrayMetadata.CoreArrayMetadata shardMetadata =
                new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
                        configuration.chunkShape, arrayMetadata.dataType, arrayMetadata.parsedFillValue);

        Arrays.stream(Indexer.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape)).parallel().forEach(
                chunkCoords -> {
                    final int i = (int) Indexer.cOrderIndex(chunkCoords, Utils.toLongArray(chunksPerShard));
                    final ByteBuffer shardBytesSlice = shardBytes.slice();

                    final int chunkByteOffset = (int) shardIndex[i][0];
                    final int chunkByteLength = (int) shardIndex[i][1];
                    Array chunkArray = null;
                    final Indexer.ChunkProjection chunkProjection =
                            Indexer.computeProjection(chunkCoords, shardMetadata.shape, shardMetadata.chunkShape);
                    if (chunkByteOffset != -1 && chunkByteLength != -1) {
                        shardBytesSlice.limit(chunkByteOffset + chunkByteLength);
                        shardBytesSlice.position(chunkByteOffset);
                        final ByteBuffer chunkBytes = ByteBuffer.allocate(chunkByteLength).put(shardBytesSlice);

                        chunkArray = new CodecPipeline(configuration.codecs).decode(chunkBytes, shardMetadata);
                    }
                    if (chunkArray == null) {
                        chunkArray = shardMetadata.allocateFillValueChunk();
                    }
                    MultiArrayUtils.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray,
                            chunkProjection.outOffset,
                            chunkProjection.shape);
                });

        return outputArray;
    }


    @Override
    public ByteBuffer encode(final Array shardArray, final ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        final ArrayMetadata.CoreArrayMetadata shardMetadata =
                new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
                        configuration.chunkShape, arrayMetadata.dataType, arrayMetadata.parsedFillValue);
        final int[] chunksPerShard = getChunksPerShard(arrayMetadata);
        final int chunkCount = Arrays.stream(chunksPerShard).reduce(1, (r, a) -> r * a);

        final long[][] shardIndex = new long[chunkCount][2];
        final List<ByteBuffer> chunkBytesList = new ArrayList<>(chunkCount);

        Arrays.stream(Indexer.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape)).parallel().forEach(
                chunkCoords -> {
                    final int i = (int) Indexer.cOrderIndex(chunkCoords, Utils.toLongArray(chunksPerShard));
                    final Indexer.ChunkProjection chunkProjection =
                            Indexer.computeProjection(chunkCoords, shardMetadata.shape, shardMetadata.chunkShape);
                    final Array chunkArray = Array.factory(shardMetadata.dataType.getMA2DataType(),
                            shardMetadata.chunkShape);
                    MultiArrayUtils.copyRegion(shardArray, chunkProjection.outOffset, chunkArray,
                            chunkProjection.chunkOffset, chunkProjection.shape);
                    if (MultiArrayUtils.allValuesEqual(chunkArray, shardMetadata.parsedFillValue)) {
                        shardIndex[i][0] = -1;
                        shardIndex[i][1] = -1;
                    } else {
                        final ByteBuffer chunkBytes = new CodecPipeline(configuration.codecs).encode(chunkArray,
                                shardMetadata);
                        synchronized (chunkBytesList) {
                            int chunkByteOffset =
                                    chunkBytesList.stream().mapToInt(ByteBuffer::capacity).sum();
                            shardIndex[i][0] = (long) chunkByteOffset;
                            shardIndex[i][1] = (long) chunkBytes.capacity();
                            chunkBytesList.add(chunkBytes);
                        }
                    }
                });
        final int shardBytesLength = chunkBytesList.stream().mapToInt(ByteBuffer::capacity).sum() + chunkCount * 16 + 4;
        final ByteBuffer shardBytes = ByteBuffer.allocate(shardBytesLength);
        for (final ByteBuffer chunkBytes : chunkBytesList) {
            shardBytes.put(chunkBytes);
        }
        shardBytes.put(writeShardIndex(shardIndex));

        return shardBytes;
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
