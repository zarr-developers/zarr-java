package dev.zarr.zarrjava.v2.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v2.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "id")
public abstract class Codec implements dev.zarr.zarrjava.core.codec.Codec {

    protected ArrayMetadata.CoreArrayMetadata arrayMetadata;

    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        if (arrayMetadata == null) {
            throw new ZarrException("arrayMetadata needs to get set in for every codec");
        }
        return this.arrayMetadata;
    }

    public abstract long computeEncodedSize(long inputByteLength, ArrayMetadata.CoreArrayMetadata arrayMetadata)
            throws ZarrException;

    public void setCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException{
        this.arrayMetadata = arrayMetadata;
    }
}

