package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
    private final Map<String, Object> extraFields;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroChannel(
            @Nullable @JsonProperty("active") Boolean active,
            @Nullable @JsonProperty("coefficient") Double coefficient,
            @Nullable @JsonProperty("color") String color,
            @Nullable @JsonProperty("family") String family,
            @Nullable @JsonProperty("inverted") Boolean inverted,
            @Nullable @JsonProperty("label") String label,
            @Nullable @JsonProperty("window") OmeroWindow window,
            @Nullable @JsonAnySetter Map<String, Object> extraFields
    ) {
        this.active = active;
        this.coefficient = coefficient;
        this.color = color;
        this.family = family;
        this.inverted = inverted;
        this.label = label;
        this.window = window;
        this.extraFields = extraFields == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(extraFields));
    }

    public OmeroChannel(
            @Nullable Boolean active,
            @Nullable Double coefficient,
            @Nullable String color,
            @Nullable String family,
            @Nullable Boolean inverted,
            @Nullable String label,
            @Nullable OmeroWindow window
    ) {
        this(active, coefficient, color, family, inverted, label, window, null);
    }

    /**
     * Members zarr-java does not model, such as {@code wavelength_id}. The omero block is
     * transitional and open-ended, so they are written back unchanged.
     *
     * @return the extra fields, never {@code null}
     */
    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return extraFields;
    }
}
