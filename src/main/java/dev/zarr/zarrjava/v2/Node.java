package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import dev.zarr.zarrjava.v2.codec.CodecRegistry;

public interface Node {
  String ZARRAY = ".zarray";
  String ZGROUP = ".zgroup";

  static ObjectMapper makeObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerSubtypes(CodecRegistry.getNamedTypes());
    return objectMapper;
  }

}
