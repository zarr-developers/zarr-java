package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
public interface Codec {

  long computeEncodedSize(long inputByteLength, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException;
}

