package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import dev.zarr.zarrjava.v3.codec.core.BloscCodec;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import dev.zarr.zarrjava.v3.codec.core.Crc32cCodec;
import dev.zarr.zarrjava.v3.codec.core.GzipCodec;
import dev.zarr.zarrjava.v3.codec.core.ShardingIndexedCodec;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import dev.zarr.zarrjava.v3.codec.core.ZstdCodec;
import java.util.HashMap;
import java.util.Map;

public class CodecRegistry {

  static Map<String, Class<? extends Codec>> map = new HashMap<>();

  static {
    addType("transpose", TransposeCodec.class);
    addType("bytes", BytesCodec.class);
    addType("blosc", BloscCodec.class);
    addType("gzip", GzipCodec.class);
    addType("zstd", ZstdCodec.class);
    addType("crc32c", Crc32cCodec.class);
    addType("sharding_indexed", ShardingIndexedCodec.class);
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
