package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;

public final class CoordinatesCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final String path;
    /** Interpolation method for the {@code path} array, e.g. {@code nearest}, {@code linear} (default), {@code bspline-cubic}. */
    @Nullable public final String interpolation;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinatesCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("path") String path,
            @Nullable @JsonProperty("interpolation") String interpolation
    ) {
        super("coordinates", input, output, name);
        this.path = path;
        this.interpolation = interpolation;
    }

    public CoordinatesCoordinateTransformation(
            @Nullable String input,
            @Nullable String output,
            @Nullable String name,
            @Nullable String path
    ) {
        this(input, output, name, path, null);
    }
}
