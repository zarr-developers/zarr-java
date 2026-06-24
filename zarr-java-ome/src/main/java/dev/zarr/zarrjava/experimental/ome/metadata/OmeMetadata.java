package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** OME-Zarr metadata stored under {@code attributes["ome"]} (v0.5). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeMetadata {

    public final String version;
    @Nullable
    public final List<MultiscalesEntry> multiscales;
    @Nullable
    public final OmeroMetadata omero;
    @Nullable
    @JsonProperty("bioformats2raw.layout")
    public final Integer bioformats2rawLayout;
    @Nullable
    public final PlateMetadata plate;
    @Nullable
    public final WellMetadata well;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscales") List<MultiscalesEntry> multiscales,
            @Nullable @JsonProperty("omero") OmeroMetadata omero,
            @Nullable @JsonProperty("bioformats2raw.layout") Integer bioformats2rawLayout,
            @Nullable @JsonProperty("plate") PlateMetadata plate,
            @Nullable @JsonProperty("well") WellMetadata well
    ) {
        this.version = version;
        this.multiscales = multiscales;
        this.omero = omero;
        this.bioformats2rawLayout = bioformats2rawLayout;
        this.plate = plate;
        this.well = well;
    }

    /** Convenience constructor for multiscale images (omero/layout/plate/well all null). */
    public OmeMetadata(String version, List<MultiscalesEntry> multiscales) {
        this(version, multiscales, null, null, null, null);
    }
}
