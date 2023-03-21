package dev.zarr.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import dev.zarr.zarrjava.v3.ArrayMetadata;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({@JsonSubTypes.Type(value = ShardingIndexedCodec.class, name = "sharding_indexed"),
        @JsonSubTypes.Type(value = BloscCodec.class, name = "blosc"),
        @JsonSubTypes.Type(value = TransposeCodec.class, name = "transpose"),
        @JsonSubTypes.Type(value = EndianCodec.class, name = "endian"),
        @JsonSubTypes.Type(value = GzipCodec.class, name = "gzip")})
public abstract class Codec {
    public abstract byte[] decode(byte[] chunk, long[][] selection, ArrayMetadata arrayMetadata);

    public abstract byte[] encode(byte[] chunk, long[][] selection, ArrayMetadata arrayMetadata);
}
