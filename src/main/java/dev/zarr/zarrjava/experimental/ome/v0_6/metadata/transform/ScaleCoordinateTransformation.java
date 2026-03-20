package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class ScaleCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<Double> scale;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ScaleCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("path") String path
    ) {
        super("scale", input, output, name);
        this.scale = scale;
        this.path = path;
    }
}
