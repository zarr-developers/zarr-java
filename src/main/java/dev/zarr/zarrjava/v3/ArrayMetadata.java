package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.v3.chunkgrid.ChunkGrid;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.codec.Codec;

import java.util.Map;
import java.util.Optional;

public final class ArrayMetadata {
    @JsonProperty("zarr_format")
    public final int zarrFormat = 3;
    @JsonProperty("node_type")
    public final String nodeType = "array";

    public Map<String, Object> attributes;

    public long[] shape;

    @JsonProperty("data_type")
    public DataType dataType;

    @JsonProperty("chunk_grid")
    public ChunkGrid chunkGrid;

    @JsonProperty("chunk_key_encoding")
    public ChunkKeyEncoding chunkKeyEncoding;

    @JsonProperty("fill_value")
    public Object fillValue;

    public Optional<Codec[]> codecs;

    @JsonProperty("dimension_names")
    public Optional<String[]> dimensionNames;
}
