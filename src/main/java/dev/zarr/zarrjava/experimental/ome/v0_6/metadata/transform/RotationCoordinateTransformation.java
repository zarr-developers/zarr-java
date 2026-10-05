package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class RotationCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final List<List<Double>> rotation;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public RotationCoordinateTransformation(
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("rotation") List<List<Double>> rotation,
            @Nullable @JsonProperty("path") String path
    ) {
        super("rotation", input, output, name);
        this.rotation = rotation;
        this.path = path;
    }
}
