package dev.zarr.zarrjava.v3.chunkgrid;

import com.fasterxml.jackson.annotation.JsonProperty;


public class RegularChunkGrid extends ChunkGrid {
    public final class Configuration {
        @JsonProperty("chunk_shape")
        public int[] chunkShape;
    }

    public final String name = "regular";
    public Configuration configuration;
}
