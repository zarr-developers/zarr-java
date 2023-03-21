package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public final class GroupMetadata {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "group";

    public Map<String, Object> attributes;
}
