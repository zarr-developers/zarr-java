package com.scalableminds.zarrjava.v3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.scalableminds.zarrjava.store.StoreHandle;
import com.scalableminds.zarrjava.v3.codec.CodecRegistry;

import javax.annotation.Nonnull;

public class Node {
    public final static String ZARR_JSON = "zarr.json";

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
