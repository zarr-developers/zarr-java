package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class RotationCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<List<Double>> rotation;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RotationCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("rotation") List<List<Double>> rotation,
            @Nullable @JsonProperty("path") String path
    ) {
        super("rotation", input, output, name);
        this.rotation = rotation;
        this.path = path;
    }
}
