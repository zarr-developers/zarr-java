package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** A reference to an image within a well. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class WellImage {

    public final String path;
    @Nullable
    public final Integer acquisition;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public WellImage(
            @JsonProperty(value = "path", required = true) String path,
            @Nullable @JsonProperty("acquisition") Integer acquisition
    ) {
        this.path = path;
        this.acquisition = acquisition;
    }
}
