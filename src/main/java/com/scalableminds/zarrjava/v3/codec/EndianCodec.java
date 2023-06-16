package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.indexing.Selector;
import com.scalableminds.zarrjava.store.ValueHandle;
import com.scalableminds.zarrjava.v3.ArrayMetadata;

public class EndianCodec extends Codec {
    public final String name = "endian";
    public Configuration configuration;

    @Override
    public ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }

    @Override
    public ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata) {
        return chunk;
    }

    public static final class Configuration {
        public String endian = "little";
    }
}

