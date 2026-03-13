package dev.zarr.zarrjava.ome.v1_0.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** OME-Zarr v1.0 collection metadata stored under {@code attributes["ome"]["collection"]}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CollectionMetadata {

    @Nullable public final String name;
    public final List<NodeRef> nodes;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CollectionMetadata(
            @Nullable @JsonProperty("name") String name,
            @JsonProperty(value = "nodes", required = true) List<NodeRef> nodes
    ) {
        this.name = name;
        this.nodes = nodes;
    }
}
