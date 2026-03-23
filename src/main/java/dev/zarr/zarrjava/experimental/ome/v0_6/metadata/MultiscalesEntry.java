package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class MultiscalesEntry {

    @Nullable public final List<Axis> axes;
    public final List<Dataset> datasets;
    @Nullable public final List<CoordinateTransformation> coordinateTransformations;
    @Nullable public final List<CoordinateSystem> coordinateSystems;
    @Nullable public final String name;
    @Nullable public final String type;
    @Nullable public final Map<String, Object> metadata;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MultiscalesEntry(
            @Nullable @JsonProperty("axes") List<Axis> axes,
            @JsonProperty(value = "datasets", required = true) List<Dataset> datasets,
            @Nullable @JsonProperty("coordinateTransformations") List<CoordinateTransformation> coordinateTransformations,
            @Nullable @JsonProperty("coordinateSystems") List<CoordinateSystem> coordinateSystems,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("type") String type,
            @Nullable @JsonProperty("metadata") Map<String, Object> metadata
    ) {
        this.axes = axes;
        this.datasets = datasets;
        this.coordinateTransformations = coordinateTransformations;
        this.coordinateSystems = coordinateSystems;
        this.name = name;
        this.type = type;
        this.metadata = metadata;
    }

    public MultiscalesEntry(List<Dataset> datasets, List<CoordinateSystem> coordinateSystems, String name) {
        this(null, datasets, null, coordinateSystems, name, null, null);
    }

    /** Returns a new MultiscalesEntry with the given dataset appended. */
    public MultiscalesEntry withDataset(Dataset dataset) {
        List<Dataset> updated = new ArrayList<>(this.datasets);
        updated.add(dataset);
        return new MultiscalesEntry(axes, updated, coordinateTransformations, coordinateSystems, name, type, metadata);
    }
}
