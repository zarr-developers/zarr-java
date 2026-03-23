package dev.zarr.zarrjava.experimental.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public class ScaleCoordinateTransformation extends CoordinateTransformation {
    @Nullable public final List<Double> scale;
    @Nullable public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ScaleCoordinateTransformation(
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("path") String path
    ) {
        super("scale");
        this.scale = scale;
        this.path = path;
    }
}
