package dev.zarr.zarrjava.ome.v1_0.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** OME-Zarr v1.0 top-level wrapper stored under {@code attributes["ome"]}. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeMetadata {

    public final String version;
    /** Present when this node is a multiscale image. */
    @Nullable public final MultiscaleMetadata multiscale;
    /** Present when this node is a collection. */
    @Nullable public final CollectionMetadata collection;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeMetadata(
            @JsonProperty(value = "version", required = true) String version,
            @Nullable @JsonProperty("multiscale") MultiscaleMetadata multiscale,
            @Nullable @JsonProperty("collection") CollectionMetadata collection
    ) {
        this.version = version;
        this.multiscale = multiscale;
        this.collection = collection;
    }

    public OmeMetadata(String version, MultiscaleMetadata multiscale) {
        this(version, multiscale, null);
    }

    public OmeMetadata(String version, CollectionMetadata collection) {
        this(version, null, collection);
    }
}
