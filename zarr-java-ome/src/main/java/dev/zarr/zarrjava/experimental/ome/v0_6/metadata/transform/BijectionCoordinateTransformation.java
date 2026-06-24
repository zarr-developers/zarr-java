package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;

public final class BijectionCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final CoordinateTransformation forward;
    @Nullable public final CoordinateTransformation inverse;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BijectionCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("forward") CoordinateTransformation forward,
            @Nullable @JsonProperty("inverse") CoordinateTransformation inverse
    ) {
        super("bijection", input, output, name);
        this.forward = forward;
        this.inverse = inverse;
    }
}
