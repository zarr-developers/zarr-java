package dev.zarr.zarrjava.v3;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Group {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "group";

    public Map<String, Object> attributes;
}
