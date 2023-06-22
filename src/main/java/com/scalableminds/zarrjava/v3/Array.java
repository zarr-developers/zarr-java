package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.store.Store;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.v3.codec.CodecPipeline;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array extends Node {
    public ArrayMetadata metadata;

    public Array(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = Utils.makeObjectMapper();
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json").array(), ArrayMetadata.class);
    }

    public ucar.ma2.Array read(long[] offset, int[] shape) {
        assert offset.length == metadata.ndim();
        assert shape.length == metadata.ndim();

        ucar.ma2.Array outputArray = ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(), shape);

        final int[] chunkShape = metadata.chunkShape();
        for (long[] chunkCoords : Indexer.computeChunkCoords(metadata.shape, chunkShape, offset, shape)) {
            final Indexer.ChunkProjection chunkProjection =
                    Indexer.computeProjection(chunkCoords, metadata.shape, chunkShape, offset, shape);

            ucar.ma2.Array chunkArray = readChunk(chunkCoords);
            Indexer.copyRegion(chunkArray, chunkProjection.chunkOffset, outputArray, chunkProjection.outOffset,
                    chunkProjection.shape);
        }
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

        ucar.ma2.Array chunkArray =
                new CodecPipeline(metadata.codecs).decode(chunkHandle.read(), metadata.getCoreMetadata());
        return chunkArray;
    }


    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(", ")),
                metadata.dataType);
    }
}
