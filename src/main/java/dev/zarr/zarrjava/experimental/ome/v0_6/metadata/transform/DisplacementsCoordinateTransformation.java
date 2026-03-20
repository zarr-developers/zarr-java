package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class DisplacementsCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DisplacementsCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("path") String path
    ) {
        super("displacements", input, output, name);
        this.path = path;
    }
}
