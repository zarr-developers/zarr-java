package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.v3.chunkkeyencoding.Separator;
import java.util.Optional;

public class ArrayMetadata {

  @JsonProperty("zarr_format")
  public final int zarrFormat = 2;

  public long[] shape;
  public long[] chunks;

  @JsonProperty("dtype")
  public DataType dataType;

  public Order order;

  @JsonProperty("dimension_separator")
  public Separator dimensionSeparator;

  @JsonProperty("fill_value")
  public Object fillValue;

  public Optional<Object[]> filters;
  public Optional<Object> compressor;
}
