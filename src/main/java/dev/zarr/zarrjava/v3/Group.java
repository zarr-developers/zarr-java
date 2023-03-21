package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Group extends Node {
    public GroupMetadata metadata;

    public Group(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json", null).get(), GroupMetadata.class);
    }

    public Node get(String key) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());

        try {
            String nodeType = objectMapper.readTree(store.get(path + "/" + key + "/zarr.json", null).get()).get(
                    "node_type").asText();
            switch (nodeType) {
                case "array":
                    return new Array(store, path + "/" + key);
                case "group":
                    return new Group(store, path + "/" + key);
                default:
                    throw new UnsupportedOperationException("Unsupported node_type: " + nodeType);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Node> list() {
        return store.list(path).stream().filter(p -> !p.equals("zarr.json")).map(this::get).collect(
                Collectors.toList());
    }

    @Override
    public String toString() {
        return String.format("<Group {%s/%s}>", store, path);
    }
}
