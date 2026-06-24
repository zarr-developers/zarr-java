package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.List;

public final class MapAxisCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final List<Integer> mapAxis;
    @Nullable public final CoordinateTransformation transformation;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MapAxisCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("mapAxis") List<Integer> mapAxis,
            @Nullable @JsonProperty("transformation") CoordinateTransformation transformation
    ) {
        super("mapAxis", input, output, name);
        this.mapAxis = mapAxis;
        this.transformation = transformation;
    }
}
