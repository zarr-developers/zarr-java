package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.Store;

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

    @Override
    public String toString() {
        return String.format("<v3.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(
                        ", ")), metadata.dataType);
    }
}
