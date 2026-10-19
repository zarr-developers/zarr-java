package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.metadata.ImageLabel;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** OME-Zarr v0.6 metadata stored under {@code attributes["ome"]}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeMetadata {

    public final String version;
    @Nullable public final List<MultiscalesEntry> multiscales;
    @Nullable public final OmeroMetadata omero;
    @Nullable
    @JsonProperty("bioformats2raw.layout")
    public final Integer bioformats2rawLayout;
    @Nullable public final SceneMetadata scene;
    @Nullable public final PlateMetadata plate;
    @Nullable public final WellMetadata well;
    /** Paths of the label images, present in the metadata of a {@code labels} group. */
    @Nullable public final List<String> labels;
    /** Display/source information of a label image. */
    @Nullable
    @JsonProperty("image-label")
    public final ImageLabel imageLabel;
    /** Image paths of a bioformats2raw collection, present in the metadata of the {@code OME} group. */
    @Nullable public final List<String> series;
    private final Map<String, Object> extraFields;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscales") List<MultiscalesEntry> multiscales,
            @Nullable @JsonProperty("omero") OmeroMetadata omero,
            @Nullable @JsonProperty("bioformats2raw.layout") Integer bioformats2rawLayout,
            @Nullable @JsonProperty("scene") SceneMetadata scene,
            @Nullable @JsonProperty("plate") PlateMetadata plate,
            @Nullable @JsonProperty("well") WellMetadata well,
            @Nullable @JsonProperty("labels") List<String> labels,
            @Nullable @JsonProperty("image-label") ImageLabel imageLabel,
            @Nullable @JsonProperty("series") List<String> series,
            @Nullable @JsonAnySetter Map<String, Object> extraFields
    ) {
        this.version = version;
        this.multiscales = multiscales;
        this.omero = omero;
        this.bioformats2rawLayout = bioformats2rawLayout;
        this.scene = scene;
        this.plate = plate;
        this.well = well;
        this.labels = labels;
        this.imageLabel = imageLabel;
        this.series = series;
        this.extraFields = extraFields == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(extraFields));
    }

    public OmeMetadata(
            String version,
            @Nullable List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omero,
            @Nullable Integer bioformats2rawLayout,
            @Nullable SceneMetadata scene,
            @Nullable PlateMetadata plate,
            @Nullable WellMetadata well,
            @Nullable List<String> labels,
            @Nullable ImageLabel imageLabel,
            @Nullable List<String> series
    ) {
        this(version, multiscales, omero, bioformats2rawLayout, scene, plate, well, labels, imageLabel, series, null);
    }

    public OmeMetadata(
            String version,
            @Nullable List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omero,
            @Nullable Integer bioformats2rawLayout,
            @Nullable SceneMetadata scene,
            @Nullable PlateMetadata plate,
            @Nullable WellMetadata well
    ) {
        this(version, multiscales, omero, bioformats2rawLayout, scene, plate, well, null, null, null);
    }

    public OmeMetadata(String version, @Nullable List<MultiscalesEntry> multiscales) {
        this(version, multiscales, null, null, null, null, null);
    }

    public OmeMetadata(
            String version,
            @Nullable List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omero
    ) {
        this(version, multiscales, omero, null, null, null, null);
    }

    public OmeMetadata(
            String version,
            @Nullable List<MultiscalesEntry> multiscales,
            @Nullable OmeroMetadata omero,
            @Nullable SceneMetadata scene
    ) {
        this(version, multiscales, omero, null, scene, null, null);
    }

    /** Returns a copy with {@code multiscales} replaced and all other fields preserved. */
    public OmeMetadata withMultiscales(@Nullable List<MultiscalesEntry> multiscales) {
        return new OmeMetadata(version, multiscales, omero, bioformats2rawLayout, scene, plate, well,
                labels, imageLabel, series, extraFields);
    }

    /**
     * Members zarr-java does not model, such as the JSON-LD keys {@code @id} and {@code @type}.
     * They are written back unchanged, so that a metadata rewrite does not drop them.
     *
     * @return the extra fields, never {@code null}
     */
    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return extraFields;
    }
}
