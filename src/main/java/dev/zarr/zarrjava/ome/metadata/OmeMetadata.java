package dev.zarr.zarrjava.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** OME-Zarr metadata stored under {@code attributes["ome"]} (v0.5). */
public final class OmeMetadata {

    public final String version;
    public final List<MultiscalesEntry> multiscales;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @JsonProperty(value = "multiscales", required = true) List<MultiscalesEntry> multiscales
    ) {
        this.version = version;
        this.multiscales = multiscales;
    }
}
