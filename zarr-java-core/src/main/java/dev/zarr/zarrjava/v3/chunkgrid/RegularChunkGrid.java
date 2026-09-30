package dev.zarr.zarrjava.v3.chunkgrid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;


public class RegularChunkGrid extends ChunkGrid {

    @JsonIgnore
    public final String name = "regular";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator
    public RegularChunkGrid(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) {
        this.configuration = configuration;
    }

    public static final class Configuration {

        @Nonnull
        @JsonProperty("chunk_shape")
        public final int[] chunkShape;

        @JsonCreator
        public Configuration(
                @Nonnull @JsonProperty(value = "chunk_shape", required = true) int[] chunkShape) {
            this.chunkShape = chunkShape;
        }
    }
}
