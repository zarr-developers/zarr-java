package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class CoordinatesCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinatesCoordinateTransformation(
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("path") String path
    ) {
        super("coordinates", input, output, name);
        this.path = path;
    }
}
