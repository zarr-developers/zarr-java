package dev.zarr.zarrjava.experimental.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class IdentityCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public IdentityCoordinateTransformation(@Nullable @JsonProperty("path") String path) {
        super("identity");
        this.path = path;
    }
}
