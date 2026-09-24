package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class ScaleCoordinateTransformation
        extends dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation
        implements CoordinateTransformation {

    @Nullable public final CoordinateSystemRef input;
    @Nullable public final CoordinateSystemRef output;
    @Nullable public final String name;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ScaleCoordinateTransformation(
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("path") String path
    ) {
        super(scale, path);
        this.input = input;
        this.output = output;
        this.name = name;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public CoordinateSystemRef getInput() {
        return input;
    }

    @Override
    public CoordinateSystemRef getOutput() {
        return output;
    }

    @Override
    public String getName() {
        return name;
    }
}
