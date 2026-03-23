package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;

import javax.annotation.Nullable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class SceneMetadata {

    @Nullable @JsonProperty("coordinateTransformations")
    public final List<CoordinateTransformation> coordinateTransformations;

    @Nullable @JsonProperty("coordinateSystems")
    public final List<CoordinateSystem> coordinateSystems;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SceneMetadata(
            @Nullable @JsonProperty("coordinateTransformations")
            List<CoordinateTransformation> coordinateTransformations,
            @Nullable @JsonProperty("coordinateSystems")
            List<CoordinateSystem> coordinateSystems
    ) {
        this.coordinateTransformations = coordinateTransformations;
        this.coordinateSystems = coordinateSystems;
    }
}
