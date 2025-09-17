package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
public abstract class Codec implements dev.zarr.zarrjava.core.codec.Codec {

    protected ArrayMetadata.CoreArrayMetadata arrayMetadata;

    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        if (arrayMetadata == null) {
            throw new ZarrException("arrayMetadata needs to get set in for every codec");
        }
        return this.arrayMetadata;
    }

    public void setCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException{
        this.arrayMetadata = arrayMetadata;
    }
}

