package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;

import javax.annotation.Nullable;
import java.util.List;

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

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscales") List<MultiscalesEntry> multiscales,
            @Nullable @JsonProperty("omero") OmeroMetadata omero,
            @Nullable @JsonProperty("bioformats2raw.layout") Integer bioformats2rawLayout,
            @Nullable @JsonProperty("scene") SceneMetadata scene,
            @Nullable @JsonProperty("plate") PlateMetadata plate,
            @Nullable @JsonProperty("well") WellMetadata well
    ) {
        this.version = version;
        this.multiscales = multiscales;
        this.omero = omero;
        this.bioformats2rawLayout = bioformats2rawLayout;
        this.scene = scene;
        this.plate = plate;
        this.well = well;
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
}
