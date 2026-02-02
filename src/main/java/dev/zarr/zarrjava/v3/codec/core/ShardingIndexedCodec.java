package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata.CoreArrayMetadata;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec;
import dev.zarr.zarrjava.core.codec.CodecPipeline;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.IndexingUtils;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.Codec;
import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ShardingIndexedCodec extends ArrayBytesCodec.WithPartialDecode implements Codec {

    @JsonIgnore
    public final String name = "sharding_indexed";
    @Nonnull
    public final Configuration configuration;
    CodecPipeline codecPipeline;
    CodecPipeline indexCodecPipeline;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ShardingIndexedCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true)
            Configuration configuration
    ) throws ZarrException {
        this.configuration = configuration;
    }

    @Override
    public void setCoreArrayMetadata(CoreArrayMetadata arrayMetadata) throws ZarrException {
        super.setCoreArrayMetadata(arrayMetadata);
        final ArrayMetadata.CoreArrayMetadata shardMetadata =
                new ArrayMetadata.CoreArrayMetadata(Utils.toLongArray(arrayMetadata.chunkShape),
                        configuration.chunkShape, arrayMetadata.dataType,
                        arrayMetadata.parsedFillValue
                );
        this.codecPipeline = new CodecPipeline(configuration.codecs, shardMetadata);
        this.indexCodecPipeline = new CodecPipeline(configuration.indexCodecs, getShardIndexArrayMetadata(getChunksPerShard(arrayMetadata)));
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
    public Array decode(ByteBuffer shardBytes)
            throws ZarrException {
        return decodeInternal(new ByteBufferDataProvider(shardBytes), new long[arrayMetadata.ndim()],
                arrayMetadata.chunkShape, arrayMetadata);
    }

    @Override
    public ByteBuffer encode(final Array shardArray) throws ZarrException {
        final ArrayMetadata.CoreArrayMetadata shardMetadata = codecPipeline.arrayMetadata;
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
                                    final ByteBuffer chunkBytes = codecPipeline.encode(chunkArray);
                                    synchronized (chunkBytesList) {
                                        int chunkByteOffset = chunkBytesList.stream()
                                                .mapToInt(ByteBuffer::capacity)
                                                .sum();
                                        if (configuration.indexLocation.equals("start")) {
                                            chunkByteOffset += (int) getShardIndexSize(arrayMetadata);
                                        }
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
                .sum() + (int) getShardIndexSize(arrayMetadata);
        final ByteBuffer shardBytes = ByteBuffer.allocate(shardBytesLength);
        if (configuration.indexLocation.equals("start")) {
            shardBytes.put(indexCodecPipeline.encode(shardIndexArray));
        }
        for (final ByteBuffer chunkBytes : chunkBytesList) {
            shardBytes.put(chunkBytes);
        }
        if (configuration.indexLocation.equals("end")) {
            shardBytes.put(indexCodecPipeline.encode(shardIndexArray));
        }
        shardBytes.rewind();
        return shardBytes;
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        return inputByteLength + getShardIndexSize(arrayMetadata);
    }

    private long getShardIndexSize(CoreArrayMetadata arrayMetadata) throws ZarrException {
        return indexCodecPipeline.computeEncodedSize(
                16 * (long) Arrays.stream(getChunksPerShard(arrayMetadata)).reduce(1, (r, a) -> r * a),
                arrayMetadata
        );
    }

    private Array decodeInternal(
            DataProvider dataProvider, long[] offset, int[] shape,
            ArrayMetadata.CoreArrayMetadata arrayMetadata
    ) throws ZarrException {
        final ArrayMetadata.CoreArrayMetadata shardMetadata = codecPipeline.arrayMetadata;

        final Array outputArray = Array.factory(arrayMetadata.dataType.getMA2DataType(), shape);
        final int shardIndexByteLength = (int) getShardIndexSize(arrayMetadata);
        ByteBuffer shardIndexBytes;
        if (this.configuration.indexLocation.equals("start")) {
            shardIndexBytes = dataProvider.readPrefix(shardIndexByteLength);
        } else if (this.configuration.indexLocation.equals("end")) {
            shardIndexBytes = dataProvider.readSuffix(shardIndexByteLength);
        } else {
            throw new ZarrException("Only index_location \"start\" or \"end\" are supported.");
        }
        if (shardIndexBytes == null) {
            if (arrayMetadata.parsedFillValue != null) {
                MultiArrayUtils.fill(outputArray, arrayMetadata.parsedFillValue);
            }
            return outputArray;
        }
        final Array shardIndexArray = indexCodecPipeline.decode(shardIndexBytes);
        long[][] allChunkCoords = IndexingUtils.computeChunkCoords(shardMetadata.shape,
                shardMetadata.chunkShape, offset,
                Utils.toLongArray(shape));

        Arrays.stream(allChunkCoords)
                // .parallel()
                .forEach(
                        chunkCoords -> {
                            try {
                                final long chunkByteOffset = getValueFromShardIndexArray(shardIndexArray,
                                        chunkCoords, 0);
                                final long chunkByteLength = getValueFromShardIndexArray(shardIndexArray,
                                        chunkCoords, 1);
                                Array chunkArray = null;
                                final IndexingUtils.ChunkProjection chunkProjection =
                                        IndexingUtils.computeProjection(chunkCoords, shardMetadata.shape,
                                                shardMetadata.chunkShape, offset, Utils.toLongArray(shape)
                                        );
                                if (chunkByteOffset != -1 && chunkByteLength != -1) {
                                    final ByteBuffer chunkBytes = dataProvider.read(chunkByteOffset, chunkByteLength);
                                    if (chunkBytes == null) {
                                        throw new ZarrException(String.format("Could not load byte data for chunk %s",
                                                Arrays.toString(chunkCoords)));
                                    }
                                    chunkArray = codecPipeline.decode(chunkBytes);
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
    public Array decodePartial(StoreHandle chunkHandle, long[] offset, int[] shape) throws ZarrException {
        if (Arrays.equals(shape, arrayMetadata.chunkShape)) {
            ByteBuffer chunkBytes = chunkHandle.read();
            if (chunkBytes == null) {
                return arrayMetadata.allocateFillValueChunk();
            }
            return decodeInternal(new ByteBufferDataProvider(chunkHandle.read()), offset, shape, arrayMetadata);
        }
        return decodeInternal(new StoreHandleDataProvider(chunkHandle), offset, shape, arrayMetadata);
    }


    interface DataProvider {

        ByteBuffer read(long start, long length);

        ByteBuffer readSuffix(long suffixLength);

        ByteBuffer readPrefix(long prefixLength);
    }

    public static final class Configuration {

        @JsonProperty("chunk_shape")
        public final int[] chunkShape;
        @Nonnull
        @JsonProperty("codecs")
        public final Codec[] codecs;
        @Nonnull
        @JsonProperty("index_codecs")
        public final Codec[] indexCodecs;
        @Nonnull
        @JsonProperty("index_location")
        public String indexLocation;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(
                @JsonProperty(value = "chunk_shape", required = true) int[] chunkShape,
                @Nonnull @JsonProperty("codecs") Codec[] codecs,
                @Nonnull @JsonProperty("index_codecs") Codec[] indexCodecs,
                @JsonProperty(value = "index_location", defaultValue = "end") String indexLocation
        ) throws ZarrException {
            if (indexLocation == null) {
                indexLocation = "end";
            }
            if (!indexLocation.equals("start") && !indexLocation.equals("end")) {
                throw new ZarrException("Only index_location \"start\" or \"end\" are supported.");
            }
            this.chunkShape = chunkShape;
            this.codecs = codecs;
            this.indexCodecs = indexCodecs;
            this.indexLocation = indexLocation;
        }
    }

    static class ByteBufferDataProvider implements DataProvider {

        @Nonnull
        final ByteBuffer buffer;


        ByteBufferDataProvider(@Nonnull ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public ByteBuffer readSuffix(long suffixLength) {
            ByteBuffer bufferSlice = buffer.slice();
            bufferSlice.position((int) (bufferSlice.capacity() - suffixLength));
            return bufferSlice.slice();
        }

        public ByteBuffer readPrefix(long prefixLength) {
            ByteBuffer bufferSlice = buffer.slice();
            bufferSlice.limit((int) (prefixLength));
            return bufferSlice.slice();
        }

        @Override
        public ByteBuffer read(long start, long length) {
            ByteBuffer bufferSlice = buffer.slice();
            bufferSlice.position((int) start);
            bufferSlice.limit((int) (start + length));
            return bufferSlice.slice();
        }
    }

    static class StoreHandleDataProvider implements DataProvider {

        @Nonnull
        final StoreHandle storeHandle;

        StoreHandleDataProvider(@Nonnull StoreHandle storeHandle) {
            this.storeHandle = storeHandle;
        }


        @Override
        public ByteBuffer readSuffix(long suffixLength) {
            return storeHandle.read(-suffixLength);
        }

        @Override
        public ByteBuffer readPrefix(long prefixLength) {
            return storeHandle.read(0, prefixLength);
        }

        @Override
        public ByteBuffer read(long start, long length) {
            return storeHandle.read(start, start + length);
        }
    }

}
