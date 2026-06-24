package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** OME-Zarr HCS plate metadata. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class PlateMetadata {

    public final List<NamedEntry> columns;
    public final List<NamedEntry> rows;
    public final List<WellRef> wells;
    @Nullable
    public final List<Acquisition> acquisitions;
    @Nullable
    public final Integer field_count;
    @Nullable
    public final String name;
    @Nullable
    public final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public PlateMetadata(
            @JsonProperty(value = "columns", required = true) List<NamedEntry> columns,
            @JsonProperty(value = "rows", required = true) List<NamedEntry> rows,
            @JsonProperty(value = "wells", required = true) List<WellRef> wells,
            @Nullable @JsonProperty("acquisitions") List<Acquisition> acquisitions,
            @Nullable @JsonProperty("field_count") Integer field_count,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("version") String version
    ) {
        this.columns = columns;
        this.rows = rows;
        this.wells = wells;
        this.acquisitions = acquisitions;
        this.field_count = field_count;
        this.name = name;
        this.version = version;
    }
}
