package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
public interface Codec {

  long computeEncodedSize(long inputByteLength, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException;
}

