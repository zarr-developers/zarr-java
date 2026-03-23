package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** OMERO channel rendering metadata. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeroChannel {
    @Nullable public final Boolean active;
    @Nullable public final Double coefficient;
    @Nullable public final String color;
    @Nullable public final String family;
    @Nullable public final Boolean inverted;
    @Nullable public final String label;
    @Nullable public final OmeroWindow window;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroChannel(
            @Nullable @JsonProperty("active") Boolean active,
            @Nullable @JsonProperty("coefficient") Double coefficient,
            @Nullable @JsonProperty("color") String color,
            @Nullable @JsonProperty("family") String family,
            @Nullable @JsonProperty("inverted") Boolean inverted,
            @Nullable @JsonProperty("label") String label,
            @Nullable @JsonProperty("window") OmeroWindow window
    ) {
        this.active = active;
        this.coefficient = coefficient;
        this.color = color;
        this.family = family;
        this.inverted = inverted;
        this.label = label;
        this.window = window;
    }
}
