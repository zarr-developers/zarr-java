package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.scalableminds.zarrjava.store.ArrayValueHandle;
import com.scalableminds.zarrjava.store.BufferValueHandle;
import com.scalableminds.zarrjava.store.NoneHandle;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
public interface Codec {
    ValueHandle decode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    ValueHandle encode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    class Registry {
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

    abstract class ArrayArrayCodec implements Codec {

        public ValueHandle encode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            Array chunkArray = chunk.toArray();
            if (chunkArray == null) return new NoneHandle();
            chunkArray = innerEncode(chunkArray, arrayMetadata);
            if (chunkArray == null) return new NoneHandle();
            return new ArrayValueHandle(chunkArray);
        }

        public ValueHandle decode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            Array chunkArray = chunk.toArray();
            if (chunkArray == null) return new NoneHandle();
            chunkArray = innerDecode(chunkArray, arrayMetadata);
            if (chunkArray == null) return new NoneHandle();
            return new ArrayValueHandle(chunkArray);
        }


        public abstract Array innerEncode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

        public abstract Array innerDecode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    }

    abstract class ArrayBytesCodec implements Codec {

        public ValueHandle encode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            Array chunkArray = chunk.toArray();
            if (chunkArray == null) return new NoneHandle();
            ByteBuffer chunkBytes = innerEncode(chunkArray, arrayMetadata);
            if (chunkBytes == null) return new NoneHandle();
            return new BufferValueHandle(chunkBytes);
        }

        public ValueHandle decode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            ByteBuffer chunkBytes = chunk.toBytes();
            if (chunkBytes == null) return new NoneHandle();
            Array chunkArray = innerDecode(chunkBytes, arrayMetadata);
            if (chunkArray == null) return new NoneHandle();
            return new ArrayValueHandle(chunkArray);
        }

        public abstract ByteBuffer innerEncode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

        public abstract Array innerDecode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);
    }

    abstract class BytesBytesCodec implements Codec {

        public ValueHandle encode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            ByteBuffer chunkBytes = chunk.toBytes();
            if (chunkBytes == null) return new NoneHandle();
            chunkBytes = innerEncode(chunkBytes, arrayMetadata);
            if (chunkBytes == null) return new NoneHandle();
            return new BufferValueHandle(chunkBytes);
        }

        public ValueHandle decode(ValueHandle chunk, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
            ByteBuffer chunkBytes = chunk.toBytes();
            if (chunkBytes == null) return new NoneHandle();
            chunkBytes = innerDecode(chunkBytes, arrayMetadata);
            if (chunkBytes == null) return new NoneHandle();
            return new BufferValueHandle(chunkBytes);
        }

        public abstract ByteBuffer innerEncode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);

        public abstract ByteBuffer innerDecode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    }
}

