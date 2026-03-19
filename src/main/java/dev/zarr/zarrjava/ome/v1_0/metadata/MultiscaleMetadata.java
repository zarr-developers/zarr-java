package dev.zarr.zarrjava.ome.v1_0.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** OME-Zarr v1.0 multiscale metadata stored under {@code attributes["ome"]["multiscale"]} (singular). */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class MultiscaleMetadata {

    @Nullable public final String name;
    public final List<Level> levels;
    @Nullable public final List<CoordinateSystem> coordinateSystems;
    @Nullable public final List<Axis> axes;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public MultiscaleMetadata(
            @Nullable @JsonProperty("name") String name,
            @JsonProperty(value = "levels", required = true) List<Level> levels,
            @Nullable @JsonProperty("coordinateSystems") List<CoordinateSystem> coordinateSystems,
            @Nullable @JsonProperty("axes") List<Axis> axes
    ) {
        this.name = name;
        this.levels = levels;
        this.coordinateSystems = coordinateSystems;
        this.axes = axes;
    }

    public MultiscaleMetadata(String name, List<Level> levels, List<CoordinateSystem> coordinateSystems) {
        this(name, levels, coordinateSystems, null);
    }

    /** Returns a new MultiscaleMetadata with the given level appended. */
    public MultiscaleMetadata withLevel(Level level) {
        List<Level> updated = new ArrayList<>(this.levels);
        updated.add(level);
        return new MultiscaleMetadata(name, updated, coordinateSystems, axes);
    }
}
