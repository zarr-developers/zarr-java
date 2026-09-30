package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;

public abstract class AbstractCodec implements Codec {
    protected ArrayMetadata.CoreArrayMetadata arrayMetadata;

    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        if (arrayMetadata == null) {
            throw new ZarrException("arrayMetadata needs to get set in for every codec");
        }
        return this.arrayMetadata;
    }

    public void setCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        this.arrayMetadata = arrayMetadata;
    }
}

