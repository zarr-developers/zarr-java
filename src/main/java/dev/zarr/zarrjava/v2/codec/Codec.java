package dev.zarr.zarrjava.v2.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "id")
public interface Codec extends dev.zarr.zarrjava.core.codec.Codec {}

