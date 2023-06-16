package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.scalableminds.zarrjava.store.Store;

import java.io.IOException;

public class Group extends Node {
    public GroupMetadata metadata;

    public Group(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json", null).get().array(), GroupMetadata.class);
    }

    public Node get(String key) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());

        try {
            String nodeType = objectMapper.readTree(store.get(path + "/" + key + "/zarr.json", null).get().array()).get(
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

    @Override
    public String toString() {
        return String.format("<v3.Group {%s/%s}>", store, path);
    }
}
