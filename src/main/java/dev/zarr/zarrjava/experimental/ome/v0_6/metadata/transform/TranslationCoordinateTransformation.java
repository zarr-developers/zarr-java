package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class TranslationCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<Double> translation;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TranslationCoordinateTransformation(
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        super("translation", input, output, name);
        this.translation = translation;
        this.path = path;
    }
}
