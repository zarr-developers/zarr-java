package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class SequenceCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SequenceCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("transformations") List<CoordinateTransformation> transformations
    ) {
        super("sequence", input, output, name, null, null, null, transformations, null, null, null);
    }
}
