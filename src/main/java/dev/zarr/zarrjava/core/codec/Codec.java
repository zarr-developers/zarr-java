package dev.zarr.zarrjava.core.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.ArrayMetadata;

public interface Codec {
    void setCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata codecArrayMetadata) throws ZarrException;

    ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException;

    default long computeEncodedSize(long inputByteLength, ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        throw new ZarrException("Not implemented for " + this.getClass());
    }
}
