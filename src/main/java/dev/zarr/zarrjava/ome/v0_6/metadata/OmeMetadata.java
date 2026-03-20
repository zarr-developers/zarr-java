package dev.zarr.zarrjava.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ome.metadata.OmeroMetadata;

import javax.annotation.Nullable;
import java.util.List;

/** OME-Zarr v0.6 metadata stored under {@code attributes["ome"]}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeMetadata {

    public final String version;
    @Nullable public final List<MultiscalesEntry> multiscales;
    @Nullable public final OmeroMetadata omero;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscales") List<MultiscalesEntry> multiscales,
            @Nullable @JsonProperty("omero") OmeroMetadata omero
    ) {
        this.version = version;
        this.multiscales = multiscales;
        this.omero = omero;
    }

    public OmeMetadata(String version, @Nullable List<MultiscalesEntry> multiscales) {
        this(version, multiscales, null);
    }
}
