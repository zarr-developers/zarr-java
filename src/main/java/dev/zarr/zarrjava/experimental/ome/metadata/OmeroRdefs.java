package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** OMERO rendering defaults metadata. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeroRdefs {
    @Nullable @JsonProperty("defaultT") public final Integer defaultT;
    @Nullable @JsonProperty("defaultZ") public final Integer defaultZ;
    @Nullable public final String model;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroRdefs(
            @Nullable @JsonProperty("defaultT") Integer defaultT,
            @Nullable @JsonProperty("defaultZ") Integer defaultZ,
            @Nullable @JsonProperty("model") String model
    ) {
        this.defaultT = defaultT;
        this.defaultZ = defaultZ;
        this.model = model;
    }
}
