package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.store.FileValueHandle;
import com.scalableminds.zarrjava.store.Store;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.codec.Codec;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array extends Node {
    public ArrayMetadata metadata;

    public Array(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = Utils.makeObjectMapper();
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json", null).get().array(), ArrayMetadata.class);
    }

    public ucar.ma2.Array read(long[] offset, int[] shape) {
        assert offset.length == metadata.ndim();
        assert shape.length == metadata.ndim();

        ucar.ma2.Array outputArray = ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(), metadata.chunkShape());

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
        ValueHandle chunkHandle = new FileValueHandle(store, path + "/" + chunkKey);

        if (metadata.codecs.isPresent() && metadata.codecs.get().length > 0) {
            Codec[] codecs = metadata.codecs.get();
            for (int i = codecs.length - 1; i >= 0; --i) {
                chunkHandle = codecs[i].decode(chunkHandle, metadata.getCoreMetadata());
            }
        }
        ByteBuffer out = chunkHandle.toBytes();
        if (out == null) {
            return metadata.allocateFillValueChunk();
        }
        return ucar.ma2.Array.factory(metadata.dataType.getMA2DataType(), metadata.chunkShape(),
                (ByteBuffer) out.rewind());
    }


    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(", ")),
                metadata.dataType);
    }
}
