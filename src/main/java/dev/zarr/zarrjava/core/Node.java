package dev.zarr.zarrjava.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.codec.CodecRegistry;
import javax.annotation.Nonnull;

public abstract class Node {

  @Nonnull
  public final StoreHandle storeHandle;

  protected Node(@Nonnull StoreHandle storeHandle) {
    this.storeHandle = storeHandle;
  }

  public static ObjectMapper makeObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
    return objectMapper;
  }

}
