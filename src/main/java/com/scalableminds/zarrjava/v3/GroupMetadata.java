package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Map;

public final class GroupMetadata {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "group";

    @Nullable
    public final Map<String, Object> attributes;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GroupMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @Nullable @JsonProperty(value = "attributes") Map<String, Object> attributes) {
        assert zarrFormat == 3;
        assert nodeType.equals("group");
        this.attributes = attributes;
    }
}
