package dev.zarr.zarrjava.experimental.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public class TranslationCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<Double> translation;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TranslationCoordinateTransformation(
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        super("translation");
        this.translation = translation;
        this.path = path;
    }
}
