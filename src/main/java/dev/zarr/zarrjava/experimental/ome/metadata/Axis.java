package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Axis {

    public final String name;
    @Nullable
    public final String type;
    @Nullable
    public final String unit;
    @Nullable
    public final Boolean discrete;
    @Nullable
    @JsonProperty("long_name")
    public final String longName;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Axis(
            @JsonProperty(value = "name", required = true) String name,
            @Nullable @JsonProperty("type") String type,
            @Nullable @JsonProperty("unit") String unit,
            @Nullable @JsonProperty("discrete") Boolean discrete,
            @Nullable @JsonProperty("long_name") String longName
    ) {
        this.name = name;
        this.type = type;
        this.unit = unit;
        this.discrete = discrete;
        this.longName = longName;
    }

    public Axis(String name, @Nullable String type, @Nullable String unit) {
        this(name, type, unit, null, null);
    }
}
