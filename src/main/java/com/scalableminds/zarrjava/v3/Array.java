package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.scalableminds.zarrjava.v3.codec.Codec;
import com.scalableminds.zarrjava.indexing.Indexer;
import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.store.FileValueHandle;
import com.scalableminds.zarrjava.store.Store;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.chunkgrid.RegularChunkGrid;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array extends Node {
    public ArrayMetadata metadata;

    public Array(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json", null).get().array(), ArrayMetadata.class);
    }

    public ByteBuffer read(long[] offset, int[] shape) {
        assert offset.length == metadata.ndim();
        assert shape.length == metadata.ndim();

        final int[] chunkShape = ((RegularChunkGrid) metadata.chunkGrid).configuration.chunkShape;

        // TODO
        assert Arrays.equals(shape, chunkShape);
        for (int i = 0; i < metadata.ndim(); i++) assert offset[i] % (long) (chunkShape[i]) == 0;

        for (long[] chunkCoords : Indexer.computeChunkCoords(metadata.shape, chunkShape, shape, offset)) {
            return readChunk(chunkCoords);
        }

        return null;
    }

    public ByteBuffer readChunk(long[] chunkCoords) {
        final int[] chunkShape = metadata.chunkShape();

        for (int i = 0; i < metadata.ndim(); i++) {
            if (chunkCoords[i] < 0 || chunkCoords[i] * chunkShape[i] >= metadata.shape[i]) {
                return allocateFillValueChunk();
            }
        }

        String chunkKey = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
        ValueHandle chunkHandle = new FileValueHandle(store, path + "/" + chunkKey);

        if (metadata.codecs.isPresent() && metadata.codecs.get().length > 0) {
            Codec[] codecs = metadata.codecs.get();
            for (int i = codecs.length - 1; i >= 0; --i) {
                chunkHandle = codecs[i].decode(chunkHandle, new Selector(metadata.ndim()), metadata);
            }
        }
        ByteBuffer out = chunkHandle.toBytes();
        if (out == null) {
            return allocateFillValueChunk();
        }
        return (ByteBuffer) out.rewind();
    }


    private ByteBuffer allocateFillValueChunk() {
        int byteLength = metadata.chunkByteLength();
        ByteBuffer fillValueBytes = ArrayMetadata.getFillValueBytes(metadata.fillValue, metadata.dataType);

        return Utils.makeByteBuffer(byteLength, b -> {
            for (int i = 0; i < metadata.chunkSize(); i++) {
                b.put(fillValueBytes);
            }
            return b;
        });
    }

    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(", ")),
                metadata.dataType);
    }
}
