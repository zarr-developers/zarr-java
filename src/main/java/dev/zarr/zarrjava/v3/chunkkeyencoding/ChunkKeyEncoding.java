package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.ZarrException;


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DefaultChunkKeyEncoding.class, name = "default"),
        @JsonSubTypes.Type(value = V2ChunkKeyEncoding.class, name = "v2"),
        @JsonSubTypes.Type(value = TemplateChunkKeyEncoding.class, name = "template")
})
public abstract class ChunkKeyEncoding implements dev.zarr.zarrjava.core.chunkkeyencoding.ChunkKeyEncoding {


    public abstract String[] encodeChunkKey(long[] chunkCoords);

    /**
     * Checks that this encoding can encode chunk coordinates of an array with {@code ndim} dimensions.
     */
    public void validate(int ndim) throws ZarrException {
    }

}
