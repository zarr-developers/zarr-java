package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Forward-compatibility fallback for transform types not yet modeled explicitly.
 *
 * <p>Used to preserve unknown/extension transform metadata during deserialization
 * rather than rejecting the entire OME payload.
 */
public final class GenericCoordinateTransformation extends BaseCoordinateTransformation {
    public final Map<String, Object> raw = new LinkedHashMap<>();

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GenericCoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name
    ) {
        super(type, input, output, name);
    }

    @JsonAnySetter
    public void capture(String key, Object value) {
        raw.put(key, value);
    }
}
