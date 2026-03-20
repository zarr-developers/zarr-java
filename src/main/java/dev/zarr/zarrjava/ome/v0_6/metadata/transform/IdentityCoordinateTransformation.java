package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class IdentityCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public IdentityCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("path") String path
    ) {
        super("identity", input, output, null, null, null, path, null, null, null, null);
    }
}
