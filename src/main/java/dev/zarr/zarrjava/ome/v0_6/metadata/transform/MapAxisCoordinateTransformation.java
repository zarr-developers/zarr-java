package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class MapAxisCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<Integer> mapAxis;
    @Nullable public final CoordinateTransformation transformation;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MapAxisCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("mapAxis") List<Integer> mapAxis,
            @Nullable @JsonProperty("transformation") CoordinateTransformation transformation
    ) {
        super("mapAxis", input, output, name);
        this.mapAxis = mapAxis;
        this.transformation = transformation;
    }
}
