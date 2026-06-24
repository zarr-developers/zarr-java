package dev.zarr.zarrjava.v2.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v2.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "id")
public interface Codec extends dev.zarr.zarrjava.core.codec.Codec {
    Codec evolveFromCoreArrayMetadata(ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException;
}

