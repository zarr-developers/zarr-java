package dev.zarr.zarrjava.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CoordinateSystemRef {

    @Nullable public final String path;
    public final String name;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinateSystemRef(
            @Nullable @JsonProperty("path") String path,
            @JsonProperty(value = "name", required = true) String name
    ) {
        this.path = path;
        this.name = name;
    }

    public String canonicalId() {
        return (path == null || path.isEmpty() ? "." : path) + "#" + name;
    }
}
