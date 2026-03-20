package dev.zarr.zarrjava.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class SceneMetadata {

    @Nullable @JsonProperty("coordinateTransformations")
    public final List<SceneCoordinateTransformation> coordinateTransformations;

    @Nullable @JsonProperty("coordinateSystems")
    public final List<CoordinateSystem> coordinateSystems;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SceneMetadata(
            @Nullable @JsonProperty("coordinateTransformations")
            List<SceneCoordinateTransformation> coordinateTransformations,
            @Nullable @JsonProperty("coordinateSystems")
            List<CoordinateSystem> coordinateSystems
    ) {
        this.coordinateTransformations = coordinateTransformations;
        this.coordinateSystems = coordinateSystems;
    }
}
