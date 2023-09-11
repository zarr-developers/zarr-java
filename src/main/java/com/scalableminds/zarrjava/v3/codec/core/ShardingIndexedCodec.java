package com.scalableminds.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.utils.IndexingUtils;
import com.scalableminds.zarrjava.utils.MultiArrayUtils;
import com.scalableminds.zarrjava.utils.Utils;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.DataType;
import com.scalableminds.zarrjava.v3.codec.ArrayBytesCodec;
import com.scalableminds.zarrjava.v3.codec.Codec;
import com.scalableminds.zarrjava.v3.codec.CodecPipeline;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;


public class ShardingIndexedCodec implements ArrayBytesCodec, ArrayBytesCodec.WithPartialEncode,
    ArrayBytesCodec.WithPartialDecode {

  public final String name = "sharding_indexed";
  @Nonnull
  public final Configuration configuration;
  final CodecPipeline codecPipeline;
  final CodecPipeline indexCodecPipeline;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public ShardingIndexedCodec(
      @Nonnull @JsonProperty(value = "configuration", required = true)
      Configuration configuration
  ) throws ZarrException {
    this.configuration = configuration;
    this.codecPipeline = new CodecPipeline(configuration.codecs);
    this.indexCodecPipeline = new CodecPipeline(configuration.indexCodecs);
  }

  ArrayMetadata.CoreArrayMetadata getShardIndexArrayMetadata(int[] chunksPerShard) {
    int[] indexShape = extendArrayBy1(chunksPerShard, 2);
    return new ArrayMetadata.CoreArrayMetadata(
        Utils.toLongArray(indexShape), indexShape, DataType.UINT64, -1);
  }

  public int[] getChunksPerShard(ArrayMetadata.CoreArrayMetadata arrayMetadata) {
    final int ndim = arrayMetadata.ndim();
    final int[] chunksPerShard = new int[ndim];
    for (int dimIdx = 0; dimIdx < ndim; dimIdx++) {
      chunksPerShard[dimIdx] =
          arrayMetadata.chunkShape[dimIdx] / configuration.chunkShape[dimIdx];
    }
    return chunksPerShard;
  }

  int[] extendArrayBy1(int[] array, int value) {
    int[] out = new int[array.length + 1];
    System.arraycopy(array, 0, out, 0, array.length);
    out[out.length - 1] = value;
    return out;
  }

  long[] extendArrayBy1(long[] array, long value) {
    long[] out = new long[array.length + 1];
    System.arraycopy(array, 0, out, 0, array.length);
    out[out.length - 1] = value;
    return out;
  }

  long getValueFromShardIndexArray(Array shardIndexArray, long[] chunkCoords, int idx) {
    return shardIndexArray.getLong(
        shardIndexArray.getIndex()
            .set(Utils.toIntArray(extendArrayBy1(chunkCoords, idx))));
  }

  void setValueFromShardIndexArray(Array shardIndexArray, long[] chunkCoords, int idx, long value) {
    shardIndexArray.setLong(
        shardIndexArray.getIndex()
            .set(Utils.toIntArray(extendArrayBy1(chunkCoords, idx))), value);
  }

  @Override
  public Array decode(ByteBuffer shardBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException {
    final Array outputArray = Array.factory(arrayMetadata.dataType.getMA2DataType(),
        arrayMetadata.chunkShape);

    final int[] chunksPerShard = getChunksPerShard(arrayMetadata);
    final int chunkCount = Arrays.stream(chunksPerShard)
        .reduce(1, (r, a) -> r * a);

    final int shardIndexByteLength = chunkCount * 16 + 4;
    shardBytes.position(shardBytes.capacity() - shardIndexByteLength);
    final ByteBuffer shardIndexBytes = shardBytes.slice();
    final Array shardIndexArray = indexCodecPipeline.decode(
        shardIndexBytes,
        getShardIndexArrayMetadata(chunksPerShard)
    );

    shardBytes.position(0);

    final ArrayMetadata.CoreArrayMetadata shardMetadata =
        new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
            configuration.chunkShape, arrayMetadata.dataType,
            arrayMetadata.parsedFillValue
        );

    Arrays.stream(
            IndexingUtils.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape))
        .parallel()
        .forEach(
            chunkCoords -> {
              try {
                final ByteBuffer shardBytesSlice = shardBytes.slice();

                final long chunkByteOffset = getValueFromShardIndexArray(shardIndexArray,
                    chunkCoords, 0);
                final long chunkByteLength = getValueFromShardIndexArray(shardIndexArray,
                    chunkCoords, 1);
                Array chunkArray = null;
                final IndexingUtils.ChunkProjection chunkProjection =
                    IndexingUtils.computeProjection(chunkCoords, shardMetadata.shape,
                        shardMetadata.chunkShape
                    );
                if (chunkByteOffset != -1 && chunkByteLength != -1) {
                  shardBytesSlice.limit((int) (chunkByteOffset + chunkByteLength));
                  shardBytesSlice.position((int) chunkByteOffset);
                  final ByteBuffer chunkBytes = ByteBuffer.allocate((int) chunkByteLength)
                      .put(shardBytesSlice);

                  chunkArray = codecPipeline.decode(chunkBytes, shardMetadata);
                }
                if (chunkArray == null) {
                  chunkArray = shardMetadata.allocateFillValueChunk();
                }
                MultiArrayUtils.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray,
                    chunkProjection.outOffset, chunkProjection.shape
                );
              } catch (ZarrException e) {
                throw new RuntimeException(e);
              }
            });

    return outputArray;
  }


  @Override
  public ByteBuffer encode(final Array shardArray,
      final ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
    final ArrayMetadata.CoreArrayMetadata shardMetadata =
        new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
            configuration.chunkShape, arrayMetadata.dataType,
            arrayMetadata.parsedFillValue
        );
    final int[] chunksPerShard = getChunksPerShard(arrayMetadata);
    final int chunkCount = Arrays.stream(chunksPerShard)
        .reduce(1, (r, a) -> r * a);

    final Array shardIndexArray = Array.factory(ucar.ma2.DataType.ULONG,
        extendArrayBy1(chunksPerShard, 2));
    final List<ByteBuffer> chunkBytesList = new ArrayList<>(chunkCount);

    Arrays.stream(
            IndexingUtils.computeChunkCoords(shardMetadata.shape, shardMetadata.chunkShape))
        .parallel()
        .forEach(
            chunkCoords -> {
              try {
                final int i =
                    (int) IndexingUtils.cOrderIndex(chunkCoords, Utils.toLongArray(chunksPerShard));
                final IndexingUtils.ChunkProjection chunkProjection =
                    IndexingUtils.computeProjection(chunkCoords, shardMetadata.shape,
                        shardMetadata.chunkShape
                    );
                final Array chunkArray =
                    shardArray.sectionNoReduce(chunkProjection.outOffset, chunkProjection.shape,
                        null
                    );
                if (MultiArrayUtils.allValuesEqual(chunkArray, shardMetadata.parsedFillValue)) {
                  setValueFromShardIndexArray(shardIndexArray, chunkCoords, 0, -1);
                  setValueFromShardIndexArray(shardIndexArray, chunkCoords, 1, -1);
                } else {
                  final ByteBuffer chunkBytes = codecPipeline.encode(chunkArray, shardMetadata);
                  synchronized (chunkBytesList) {
                    int chunkByteOffset = chunkBytesList.stream()
                        .mapToInt(ByteBuffer::capacity)
                        .sum();
                    setValueFromShardIndexArray(shardIndexArray, chunkCoords, 0, chunkByteOffset);
                    setValueFromShardIndexArray(shardIndexArray, chunkCoords, 1,
                        chunkBytes.capacity());
                    chunkBytesList.add(chunkBytes);
                  }
                }
              } catch (ZarrException | InvalidRangeException e) {
                throw new RuntimeException(e);
              }
            });
    final int shardBytesLength = chunkBytesList.stream()
        .mapToInt(ByteBuffer::capacity)
        .sum() + chunkCount * 16 + 4;
    final ByteBuffer shardBytes = ByteBuffer.allocate(shardBytesLength);
    for (final ByteBuffer chunkBytes : chunkBytesList) {
      shardBytes.put(chunkBytes);
    }
    shardBytes.put(
        indexCodecPipeline.encode(shardIndexArray, getShardIndexArrayMetadata(chunksPerShard)));
    shardBytes.rewind();
    return shardBytes;
  }

  @Override
  public ByteBuffer encodePartial(
      Array chunkArray, long[] offset, int[] shape,
      ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) {
    // TODO
    return null;
  }

  @Override
  public Array partialDecode(
      ByteBuffer chunkBytes, long[] offset, int[] shape,
      ArrayMetadata.CoreArrayMetadata arrayMetadata
  ) {
    // TODO
    return null;
  }

  public static final class Configuration {

    @JsonProperty("chunk_shape")
    public final int[] chunkShape;
    @Nullable
    public final Codec[] codecs;
    @Nullable
    public final Codec[] indexCodecs;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Configuration(
        @JsonProperty(value = "chunk_shape", required = true) int[] chunkShape,
        @Nonnull @JsonProperty("codecs") Codec[] codecs,
        @Nonnull @JsonProperty("index_codecs") Codec[] indexCodecs
    ) {
      this.chunkShape = chunkShape;
      this.codecs = codecs;
      this.indexCodecs = indexCodecs;
    }
  }
}
