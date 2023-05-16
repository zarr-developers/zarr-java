package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.indexing.Selector;
import dev.zarr.zarrjava.store.ValueHandle;
import dev.zarr.zarrjava.v3.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({@JsonSubTypes.Type(value = ShardingIndexedCodec.class, name = "sharding_indexed"),
        @JsonSubTypes.Type(value = BloscCodec.class, name = "blosc"),
        @JsonSubTypes.Type(value = TransposeCodec.class, name = "transpose"),
        @JsonSubTypes.Type(value = EndianCodec.class, name = "endian"),
        @JsonSubTypes.Type(value = GzipCodec.class, name = "gzip")})
public abstract class Codec {
    public abstract ValueHandle decode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata);

    public abstract ValueHandle encode(ValueHandle chunk, Selector selector, ArrayMetadata arrayMetadata);
}
