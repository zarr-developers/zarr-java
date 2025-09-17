package dev.zarr.zarrjava.v2.codec;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import dev.zarr.zarrjava.v2.codec.core.*;

import java.util.HashMap;
import java.util.Map;

public class CodecRegistry {

  static Map<String, Class<? extends Codec>> map = new HashMap<>();

  static {
    addType("blosc", BloscCodec.class);
    addType("zlib", ZlibCodec.class);
  }

  public static void addType(String name, Class<? extends Codec> codecClass) {
    map.put(name, codecClass);
  }

  public static NamedType[] getNamedTypes() {
    return map.entrySet()
        .stream()
        .map(entry -> new NamedType(entry.getValue(), entry.getKey()))
        .toArray(
            NamedType[]::new);
  }
}
