package dev.zarr.conventions.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.conventions.ome.metadata.Axis;
import dev.zarr.conventions.ome.v0_6.metadata.transform.CoordinateSystemRef;
import dev.zarr.conventions.ome.v0_6.metadata.transform.CoordinateTransformation;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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
    private final Map<String, Object> extraFields;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MultiscalesEntry(
            @Nullable @JsonProperty("axes") List<Axis> axes,
            @JsonProperty(value = "datasets", required = true) List<Dataset> datasets,
            @Nullable @JsonProperty("coordinateTransformations") List<CoordinateTransformation> coordinateTransformations,
            @Nullable @JsonProperty("coordinateSystems") List<CoordinateSystem> coordinateSystems,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("type") String type,
            @Nullable @JsonProperty("metadata") Map<String, Object> metadata,
            @Nullable @JsonAnySetter Map<String, Object> extraFields
    ) {
        this.axes = axes;
        this.datasets = datasets;
        this.coordinateTransformations = coordinateTransformations;
        this.coordinateSystems = coordinateSystems;
        this.name = name;
        this.type = type;
        this.metadata = metadata;
        this.extraFields = extraFields == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(extraFields));
    }

    public MultiscalesEntry(
            @Nullable List<Axis> axes,
            List<Dataset> datasets,
            @Nullable List<CoordinateTransformation> coordinateTransformations,
            @Nullable List<CoordinateSystem> coordinateSystems,
            @Nullable String name,
            @Nullable String type,
            @Nullable Map<String, Object> metadata
    ) {
        this(axes, datasets, coordinateTransformations, coordinateSystems, name, type, metadata, null);
    }

    public MultiscalesEntry(List<Dataset> datasets, List<CoordinateSystem> coordinateSystems, String name) {
        this(null, datasets, null, coordinateSystems, name, null, null);
    }

    /**
     * The coordinate system the datasets map into (named by the {@code output} of their
     * transformations), or the first listed coordinate system if no dataset names one.
     */
    @Nullable
    public CoordinateSystem intrinsicCoordinateSystem() {
        if (coordinateSystems == null || coordinateSystems.isEmpty()) {
            return null;
        }
        for (Dataset dataset : datasets) {
            if (dataset.coordinateTransformations == null || dataset.coordinateTransformations.isEmpty()) {
                continue;
            }
            CoordinateSystemRef output = dataset.coordinateTransformations.get(0).getOutput();
            if (output == null || output.name == null) {
                continue;
            }
            for (CoordinateSystem coordinateSystem : coordinateSystems) {
                if (output.name.equals(coordinateSystem.name)) {
                    return coordinateSystem;
                }
            }
            break;
        }
        return coordinateSystems.get(0);
    }

    /** Returns a new MultiscalesEntry with the given dataset appended. */
    public MultiscalesEntry withDataset(Dataset dataset) {
        List<Dataset> updated = new ArrayList<>(this.datasets);
        updated.add(dataset);
        return new MultiscalesEntry(axes, updated, coordinateTransformations, coordinateSystems, name, type, metadata, extraFields);
    }

    /**
     * Members zarr-java does not model, such as the JSON-LD key {@code @id}.
     * They are written back unchanged, so that a metadata rewrite does not drop them.
     *
     * @return the extra fields, never {@code null}
     */
    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return extraFields;
    }
}
