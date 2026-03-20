package dev.zarr.zarrjava.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public final class ScaleCoordinateTransformation extends CoordinateTransformation {
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ScaleCoordinateTransformation(
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("path") String path
    ) {
        super("scale", scale, null, path);
    }
}
