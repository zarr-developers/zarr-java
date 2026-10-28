package dev.zarr.conventions.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public final class DisplacementsCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final String path;
    /** Interpolation method for the {@code path} array, e.g. {@code nearest}, {@code linear} (default), {@code bspline-cubic}. */
    @Nullable public final String interpolation;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DisplacementsCoordinateTransformation(
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("path") String path,
            @Nullable @JsonProperty("interpolation") String interpolation
    ) {
        super("displacements", input, output, name);
        this.path = path;
        this.interpolation = interpolation;
    }

    public DisplacementsCoordinateTransformation(
            @Nullable CoordinateSystemRef input,
            @Nullable CoordinateSystemRef output,
            @Nullable String name,
            @Nullable String path
    ) {
        this(input, output, name, path, null);
    }
}
