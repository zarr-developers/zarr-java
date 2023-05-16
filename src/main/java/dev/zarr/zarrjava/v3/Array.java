package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.indexing.Indexer;
import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.FileValueHandle;
import dev.zarr.zarrjava.store.Store;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.chunkgrid.RegularChunkGrid;
import dev.zarr.zarrjava.v3.codec.Codec;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array extends Node {
    public ArrayMetadata metadata;

    public Array(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json", null).get(), ArrayMetadata.class);
    }

    public byte[] read(long[] offset, int[] shape) {
        assert offset.length == metadata.ndim();
        assert shape.length == metadata.ndim();

        final int[] chunkShape = ((RegularChunkGrid) metadata.chunkGrid).configuration.chunkShape;

        // TODO
        assert Arrays.equals(shape, chunkShape);
        for (int i = 0; i < metadata.ndim(); i++) assert offset[i] % (long) (chunkShape[i]) == 0;

        for (long[] chunkCoords : Indexer.computeChunkCoords(metadata.shape, chunkShape, shape, offset)) {
            String chunkKey = metadata.chunkKeyEncoding.encodeChunkKey(chunkCoords);
            ValueHandle chunkHandle = new FileValueHandle(store, path + "/" + chunkKey);

            if (metadata.codecs.isPresent() && metadata.codecs.get().length > 0) {
                Codec[] codecs = metadata.codecs.get();
                for (int i = codecs.length - 1; i >= 0; --i) {
                    chunkHandle = codecs[i].decode(chunkHandle, new Selector(metadata.ndim()), metadata);
                }
            }
            return chunkHandle.toBytes();
        }

        return null;
    }

    private byte[] allocateFillValueChunk() {
        int chunkSize =
                Arrays.stream(((RegularChunkGrid) metadata.chunkGrid).configuration.chunkShape).reduce(1,
                        (acc, a) -> acc * a);
        int byteLength = metadata.dataType.getByteCount() * (int) chunkSize;
        byte[] out = new byte[metadata.chunkByteLength()];
        // TODO
        Arrays.fill(out, (byte) 1);
        return out;
    }

    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(
                        ", ")), metadata.dataType);
    }
}
