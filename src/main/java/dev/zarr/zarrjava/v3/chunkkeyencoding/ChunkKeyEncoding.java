package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DefaultChunkKeyEncoding.class, name = "default"),
        @JsonSubTypes.Type(value = V2ChunkKeyEncoding.class, name = "v2")
})
public abstract class ChunkKeyEncoding implements dev.zarr.zarrjava.core.chunkkeyencoding.ChunkKeyEncoding {


    public abstract String[] encodeChunkKey(long[] chunkCoords);

}
