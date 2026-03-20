package dev.zarr.zarrjava.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/** Omero display metadata stored in OME-Zarr attributes. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeroMetadata {

    @Nullable
    public final Integer id;
    @Nullable
    public final String version;
    @Nullable
    public final String name;
    @Nullable
    public final List<Map<String, Object>> channels;
    @Nullable
    public final Map<String, Object> rdefs;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroMetadata(
            @Nullable @JsonProperty("id") Integer id,
            @Nullable @JsonProperty("version") String version,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("channels") List<Map<String, Object>> channels,
            @Nullable @JsonProperty("rdefs") Map<String, Object> rdefs
    ) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.channels = channels;
        this.rdefs = rdefs;
    }

    public OmeroMetadata(
            @Nullable List<Map<String, Object>> channels,
            @Nullable Map<String, Object> rdefs
    ) {
        this(null, null, null, channels, rdefs);
    }
}
