package dev.zarr.zarrjava.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A named entry used for plate rows/columns. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class NamedEntry {

    public final String name;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public NamedEntry(
            @JsonProperty(value = "name", required = true) String name
    ) {
        this.name = name;
    }
}
