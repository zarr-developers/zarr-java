package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalableminds.zarrjava.store.Store;

import java.io.IOException;
import java.util.Iterator;

public class Group extends Node {
    public GroupMetadata metadata;

    public Group(Store store, String path) throws IOException {
        super(store, path);

        ObjectMapper objectMapper = Utils.makeObjectMapper();
        this.metadata = objectMapper.readValue(store.get(path + "/zarr.json").array(), GroupMetadata.class);
    }

    public Node get(String key) {
        ObjectMapper objectMapper = Utils.makeObjectMapper();

        try {
            String nodeType = objectMapper.readTree(store.get(path + "/" + key + "/zarr.json").array()).get(
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

    public Iterator<Node> list() {
        if (!(store instanceof Store.ListableStore)) {
            throw new UnsupportedOperationException("The underlying store does not support listing.");
        }
        return Utils.asStream(((Store.ListableStore) store).list(path)).map(this::get).iterator();
    }

    @Override
    public String toString() {
        return String.format("<v3.Group {%s/%s}>", store, path);
    }
}
