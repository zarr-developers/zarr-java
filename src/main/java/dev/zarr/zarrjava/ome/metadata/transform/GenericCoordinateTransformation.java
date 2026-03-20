package dev.zarr.zarrjava.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class GenericCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GenericCoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        super(type, scale, translation, path);
    }
}
