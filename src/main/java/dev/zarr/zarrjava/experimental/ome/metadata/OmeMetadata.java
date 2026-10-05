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
    /** Paths of the label images, present in the metadata of a {@code labels} group. */
    @Nullable
    public final List<String> labels;
    /** Display/source information of a label image. */
    @Nullable
    @JsonProperty("image-label")
    public final ImageLabel imageLabel;
    /** Image paths of a bioformats2raw collection, present in the metadata of the {@code OME} group. */
    @Nullable
    public final List<String> series;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscales") List<MultiscalesEntry> multiscales,
            @Nullable @JsonProperty("omero") OmeroMetadata omero,
            @Nullable @JsonProperty("bioformats2raw.layout") Integer bioformats2rawLayout,
            @Nullable @JsonProperty("plate") PlateMetadata plate,
            @Nullable @JsonProperty("well") WellMetadata well,
            @Nullable @JsonProperty("labels") List<String> labels,
            @Nullable @JsonProperty("image-label") ImageLabel imageLabel,
            @Nullable @JsonProperty("series") List<String> series
    ) {
        this.version = version;
        this.multiscales = multiscales;
        this.omero = omero;
        this.bioformats2rawLayout = bioformats2rawLayout;
        this.plate = plate;
        this.well = well;
        this.labels = labels;
        this.imageLabel = imageLabel;
        this.series = series;
    }

    public OmeMetadata(
            String version,
            @Nullable List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omero,
            @Nullable Integer bioformats2rawLayout,
            @Nullable PlateMetadata plate,
            @Nullable WellMetadata well
    ) {
        this(version, multiscales, omero, bioformats2rawLayout, plate, well, null, null, null);
    }

    /** Convenience constructor for multiscale images (omero/layout/plate/well all null). */
    public OmeMetadata(String version, List<MultiscalesEntry> multiscales) {
        this(version, multiscales, null, null, null, null);
    }

    /** Returns a copy with {@code multiscales} replaced and all other fields preserved. */
    public OmeMetadata withMultiscales(@Nullable List<MultiscalesEntry> multiscales) {
        return new OmeMetadata(version, multiscales, omero, bioformats2rawLayout, plate, well,
                labels, imageLabel, series);
    }
}
