package dev.zarr.zarrjava.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class IdentityCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public IdentityCoordinateTransformation(@Nullable @JsonProperty("path") String path) {
        super("identity", null, null, path);
    }
}
