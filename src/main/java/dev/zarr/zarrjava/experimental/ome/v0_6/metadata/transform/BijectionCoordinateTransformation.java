package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class BijectionCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final CoordinateTransformation forward;
    @Nullable public final CoordinateTransformation inverse;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BijectionCoordinateTransformation(
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("forward") CoordinateTransformation forward,
            @Nullable @JsonProperty("inverse") CoordinateTransformation inverse
    ) {
        super("bijection", input, output, name);
        this.forward = forward;
        this.inverse = inverse;
    }
}
