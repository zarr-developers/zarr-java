package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.indexing.MultiArrayUtils;
import com.scalableminds.zarrjava.store.Store;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.v3.codec.CodecPipeline;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array extends Node {
    public ArrayMetadata metadata;

    Array(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = Utils.makeObjectMapper();
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json").array(), ArrayMetadata.class);
    }

    public static Array open(Store store, String path) throws IOException {
        return new Array(store, path);
    }

    public static Array create(Store store, String path, ArrayMetadata arrayMetadata) throws IOException {
        ObjectMapper objectMapper = Utils.makeObjectMapper();
        ByteBuffer metadataBytes = ByteBuffer.wrap(objectMapper.writeValueAsBytes(arrayMetadata));
        System.out.println(Charset.defaultCharset().decode(metadataBytes.slice()));
        store.set(path + "/zarr.json", metadataBytes);
        return new Array(store, path);
    }

    public ucar.ma2.Array read(long[] offset, int[] shape) {
        assert offset.length == metadata.ndim();
        assert shape.length == metadata.ndim();

        ucar.ma2.Array outputArray = ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(), shape);

        final int[] chunkShape = metadata.chunkShape();
        Arrays.stream(Indexer.computeChunkCoords(metadata.shape, chunkShape, offset, shape)).parallel().forEach(
                chunkCoords -> {
                    final Indexer.ChunkProjection chunkProjection =
                            Indexer.computeProjection(chunkCoords, metadata.shape, chunkShape, offset, shape);

                    ucar.ma2.Array chunkArray = readChunk(chunkCoords);
                    MultiArrayUtils.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray,
                            chunkProjection.outOffset, chunkProjection.shape);
                });
        return outputArray;
    }

    @Nonnull
    public ucar.ma2.Array readChunk(long[] chunkCoords) {
        final int[] chunkShape = metadata.chunkShape();

        for (int dimIdx = 0; dimIdx < metadata.ndim(); dimIdx++) {
            if (chunkCoords[dimIdx] < 0 || chunkCoords[dimIdx] * chunkShape[dimIdx] >= metadata.shape[dimIdx]) {
                return metadata.allocateFillValueChunk();
            }
        }

        String chunkKey = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
        StoreHandle chunkHandle = new StoreHandle(store, path + "/" + chunkKey);

        ByteBuffer chunkBytes = chunkHandle.read();
        if (chunkBytes == null) {
            return metadata.allocateFillValueChunk();
        }

        ucar.ma2.Array chunkArray = new CodecPipeline(metadata.codecs).decode(chunkBytes, metadata.coreArrayMetadata);
        return chunkArray;
    }

    public void write(long[] offset, ucar.ma2.Array array) {
        assert offset.length == metadata.ndim();

        int[] shape = array.getShape();

        final int[] chunkShape = metadata.chunkShape();
        Arrays.stream(Indexer.computeChunkCoords(metadata.shape, chunkShape, offset, shape)).parallel().forEach(
                chunkCoords -> {
                    final Indexer.ChunkProjection chunkProjection =
                            Indexer.computeProjection(chunkCoords, metadata.shape, chunkShape, offset, shape);

                    ucar.ma2.Array chunkArray =
                            (Indexer.isFullChunk(chunkProjection.chunkOffset, chunkProjection.shape, chunkShape)) ?
                                    ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(), chunkShape) :
                                    readChunk(chunkCoords);
                    MultiArrayUtils.copyRegion(array, chunkProjection.outOffset, chunkArray,
                            chunkProjection.chunkOffset, chunkProjection.shape);
                    writeChunk(chunkCoords, chunkArray);
                });
    }

    public void writeChunk(long[] chunkCoords, ucar.ma2.Array chunkArray) {
        String chunkKey = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
        StoreHandle chunkHandle = new StoreHandle(store, path + "/" + chunkKey);

        if (MultiArrayUtils.allValuesEqual(chunkArray, metadata.parsedFillValue)) {
            chunkHandle.delete();
        } else {
            ByteBuffer chunkBytes = new CodecPipeline(metadata.codecs).encode(chunkArray, metadata.coreArrayMetadata);
            chunkHandle.set(chunkBytes);
        }
    }


    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(", ")),
                metadata.dataType);
    }
}
