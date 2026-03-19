package dev.zarr.zarrjava.ome.v1_0.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** A reference to a child node within a v1.0 collection. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NodeRef {

    public final String type;  // "multiscale" | "collection"
    public final String path;
    @Nullable public final String name;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public NodeRef(
            @JsonProperty(value = "type", required = true) String type,
            @JsonProperty(value = "path", required = true) String path,
            @Nullable @JsonProperty("name") String name
    ) {
        this.type = type;
        this.path = path;
        this.name = name;
    }

    public NodeRef(String type, String path) {
        this(type, path, null);
    }
}
