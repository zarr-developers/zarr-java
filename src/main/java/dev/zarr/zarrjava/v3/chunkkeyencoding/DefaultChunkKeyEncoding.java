package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class DefaultChunkKeyEncoding extends ChunkKeyEncoding {

  @JsonIgnore
  public final String name = "default";
  @Nonnull
  public final Configuration configuration;

  @JsonCreator
  public DefaultChunkKeyEncoding(
      @JsonProperty(value = "configuration") Configuration configuration
  ) {
      if (configuration == null) {
        this.configuration = new Configuration(Separator.SLASH);
      } else {
        this.configuration = configuration;
      }
  }


  @Override
  public String[] encodeChunkKey(long[] chunkCoords) {
    Stream<String> keys = Stream.concat(Stream.of("c"), Arrays.stream(chunkCoords)
        .mapToObj(Long::toString));
    if (configuration.separator == Separator.SLASH) {
      return keys.toArray(String[]::new);
    }
    return new String[]{keys.collect(Collectors.joining(this.configuration.separator.getValue()))};
  }

  public static final class Configuration {

    @Nonnull
    public final Separator separator;

    @JsonCreator
    public Configuration(
        @Nonnull @JsonProperty(value = "separator", defaultValue = "/") Separator separator) {
      this.separator = separator;
    }
  }
}
