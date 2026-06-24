package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
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
