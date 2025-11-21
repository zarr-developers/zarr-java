package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class V2ChunkKeyEncoding extends ChunkKeyEncoding {

  @JsonIgnore
  public final String name = "v2";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator
  public V2ChunkKeyEncoding(
      @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
  ) {
    this.configuration = configuration;
  }

  @Override
  public String[] encodeChunkKey(long[] chunkCoords) {
    Stream<String> keys = Arrays.stream(chunkCoords)
        .mapToObj(Long::toString);
    if (configuration.separator == Separator.SLASH) {
      return keys.toArray(String[]::new);
    }
    return new String[]{keys.collect(Collectors.joining(this.configuration.separator.getValue()))};
  }

  public static final class Configuration {

    public final Separator separator;

    @JsonCreator
    public Configuration(
        @Nonnull @JsonProperty(value = "separator", defaultValue = ".") Separator separator) {
      this.separator = separator;
    }
  }
}

