package dev.zarr.zarrjava.experimental.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Forward-compatibility fallback for transform types not yet modeled explicitly.
 *
 * <p>Readers can deserialize and retain metadata for unknown/extension transform types
 * without failing hard.
 */
public class GenericCoordinateTransformation extends CoordinateTransformation {
    public final Map<String, Object> raw = new LinkedHashMap<>();

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GenericCoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type
    ) {
        super(type);
    }

    @JsonAnySetter
    public void capture(String key, Object value) {
        raw.put(key, value);
    }
}
