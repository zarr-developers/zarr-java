package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** An HCS acquisition entry within a plate. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Acquisition {

    public final int id;
    @Nullable
    public final String name;
    @Nullable
    public final Integer maximumfieldcount;
    @Nullable
    public final String description;
    @Nullable
    public final Long starttime;
    @Nullable
    public final Long endtime;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Acquisition(
            @JsonProperty(value = "id", required = true) int id,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("maximumfieldcount") Integer maximumfieldcount,
            @Nullable @JsonProperty("description") String description,
            @Nullable @JsonProperty("starttime") Long starttime,
            @Nullable @JsonProperty("endtime") Long endtime
    ) {
        this.id = id;
        this.name = name;
        this.maximumfieldcount = maximumfieldcount;
        this.description = description;
        this.starttime = starttime;
        this.endtime = endtime;
    }
}
