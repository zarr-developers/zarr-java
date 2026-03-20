package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class SequenceCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<CoordinateTransformation> transformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SequenceCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("transformations") List<CoordinateTransformation> transformations
    ) {
        super("sequence", input, output, name);
        this.transformations = transformations;
    }
}
