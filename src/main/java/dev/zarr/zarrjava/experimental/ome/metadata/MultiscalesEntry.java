package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class MultiscalesEntry {

    public final List<Axis> axes;
    public final List<Dataset> datasets;
    @Nullable
    @JsonProperty("coordinateTransformations")
    public final List<CoordinateTransformation> coordinateTransformations;
    @Nullable
    public final String name;
    @Nullable
    public final String type;
    @Nullable
    public final Map<String, Object> metadata;
    @Nullable
    public final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MultiscalesEntry(
            @JsonProperty(value = "axes", required = true) List<Axis> axes,
            @JsonProperty(value = "datasets", required = true) List<Dataset> datasets,
            @Nullable @JsonProperty("coordinateTransformations") List<CoordinateTransformation> coordinateTransformations,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("type") String type,
            @Nullable @JsonProperty("metadata") Map<String, Object> metadata,
            @Nullable @JsonProperty("version") String version
    ) {
        this.axes = axes;
        this.datasets = datasets;
        this.coordinateTransformations = coordinateTransformations;
        this.name = name;
        this.type = type;
        this.metadata = metadata;
        this.version = version;
    }

    public MultiscalesEntry(List<Axis> axes, List<Dataset> datasets) {
        this(axes, datasets, null, null, null, null, null);
    }

    /** Returns a new MultiscalesEntry with the given dataset appended. */
    public MultiscalesEntry withDataset(Dataset dataset) {
        List<Dataset> updated = new ArrayList<>(this.datasets);
        updated.add(dataset);
        return new MultiscalesEntry(axes, updated, coordinateTransformations, name, type, metadata, version);
    }
}
