package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.scalableminds.zarrjava.v3.codec.core.*;

import java.util.HashMap;
import java.util.Map;

public class CodecRegistry {
    static Map<String, Class<? extends Codec>> map = new HashMap<>();

    static {
        addType("transpose", TransposeCodec.class);
        addType("endian", EndianCodec.class);
        addType("blosc", BloscCodec.class);
        addType("gzip", GzipCodec.class);
        addType("sharding_indexed", ShardingIndexedCodec.class);
    }

    public static void addType(String name, Class<? extends Codec> codecClass) {
        map.put(name, codecClass);
    }

    public static NamedType[] getNamedTypes() {
        return map.entrySet().stream().map(entry -> new NamedType(entry.getValue(), entry.getKey())).toArray(
                NamedType[]::new);
    }
}
