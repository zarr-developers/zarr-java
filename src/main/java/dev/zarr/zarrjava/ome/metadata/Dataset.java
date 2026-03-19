package dev.zarr.zarrjava.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class Dataset {

    public final String path;
    @JsonProperty("coordinateTransformations")
    public final List<CoordinateTransformation> coordinateTransformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Dataset(
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "coordinateTransformations", required = true)
            List<CoordinateTransformation> coordinateTransformations
    ) {
        this.path = path;
        this.coordinateTransformations = coordinateTransformations;
    }
}
