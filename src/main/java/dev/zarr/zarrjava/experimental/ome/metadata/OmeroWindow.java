package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** OMERO channel window metadata. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeroWindow {
    @Nullable public final Double min;
    @Nullable public final Double max;
    @Nullable public final Double start;
    @Nullable public final Double end;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroWindow(
            @Nullable @JsonProperty("min") Double min,
            @Nullable @JsonProperty("max") Double max,
            @Nullable @JsonProperty("start") Double start,
            @Nullable @JsonProperty("end") Double end
    ) {
        this.min = min;
        this.max = max;
        this.start = start;
        this.end = end;
    }
}
