package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/** OME-Zarr HCS well metadata. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class WellMetadata {

    public final List<WellImage> images;
    @Nullable
    public final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public WellMetadata(
            @JsonProperty(value = "images", required = true) List<WellImage> images,
            @Nullable @JsonProperty("version") String version
    ) {
        this.images = images;
        this.version = version;
    }

    public WellMetadata(List<WellImage> images) {
        this(images, null);
    }
}
