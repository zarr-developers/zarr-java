package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class AffineCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<List<Double>> affine;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public AffineCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("affine") List<List<Double>> affine,
            @Nullable @JsonProperty("path") String path
    ) {
        super("affine", input, output, name);
        this.affine = affine;
        this.path = path;
    }
}
