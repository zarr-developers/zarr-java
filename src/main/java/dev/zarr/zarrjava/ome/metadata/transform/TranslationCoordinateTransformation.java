package dev.zarr.zarrjava.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class TranslationCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TranslationCoordinateTransformation(
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        super("translation", null, translation, path);
    }
}
