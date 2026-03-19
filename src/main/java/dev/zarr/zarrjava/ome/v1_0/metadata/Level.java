package dev.zarr.zarrjava.ome.v1_0.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation;

import java.util.List;

/** A single resolution level within a v1.0 multiscale image (replaces v0.6 Dataset). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Level {

    public final String path;
    public final List<CoordinateTransformation> coordinateTransformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Level(
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "coordinateTransformations", required = true)
            List<CoordinateTransformation> coordinateTransformations
    ) {
        this.path = path;
        this.coordinateTransformations = coordinateTransformations;
    }
}
