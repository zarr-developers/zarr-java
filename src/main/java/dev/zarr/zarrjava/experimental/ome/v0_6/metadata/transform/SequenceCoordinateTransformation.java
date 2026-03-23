package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.List;

public final class SequenceCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final List<CoordinateTransformation> transformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SequenceCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("transformations") List<CoordinateTransformation> transformations
    ) {
        super("sequence", input, output, name);
        this.transformations = transformations;
    }
}
