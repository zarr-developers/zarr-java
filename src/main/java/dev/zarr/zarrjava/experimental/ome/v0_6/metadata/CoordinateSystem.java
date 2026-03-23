package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CoordinateSystem {

    public final String name;
    public final List<Axis> axes;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinateSystem(
            @JsonProperty(value = "name", required = true) String name,
            @JsonProperty(value = "axes", required = true) List<Axis> axes
    ) {
        this.name = name;
        this.axes = axes;
    }
}
