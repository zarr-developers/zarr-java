package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.Store;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Array {
    public ArrayMetadata metadata;
    public Store store;
    public String path;

    public Array(Store store, String path) throws IOException {
        this.store = store;
        this.path = path;

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        this.metadata = objectMapper.readValue(store.get(path + "/.zarray", null).get(), ArrayMetadata.class);
    }

    @Override
    public String toString() {
        return String.format("<v2.Array {%s/%s} (%s) %s>", store, path,
                Arrays.stream(metadata.shape).mapToObj(Long::toString).collect(Collectors.joining(", ")),
                metadata.dataType);
    }
}
