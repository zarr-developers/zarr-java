package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Forward-compatibility fallback for transform types not yet modeled explicitly.
 *
 * <p>Used to preserve unknown/extension transform metadata during deserialization
 * rather than rejecting the entire OME payload.
 */
public final class GenericCoordinateTransformation extends CoordinateTransformation {
    public final Map<String, Object> raw = new LinkedHashMap<>();

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GenericCoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name
    ) {
        super(type, input, output, name);
    }

    @JsonAnySetter
    public void capture(String key, Object value) {
        raw.put(key, value);
    }
}
