package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class MapAxisCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MapAxisCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("mapAxis") List<Integer> mapAxis,
            @Nullable @JsonProperty("transformation") CoordinateTransformation transformation
    ) {
        super("mapAxis", input, output, name, null, null, null, null, mapAxis, null, transformation);
    }
}
