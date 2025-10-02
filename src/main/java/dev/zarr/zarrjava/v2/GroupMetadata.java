package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;

public final class GroupMetadata extends dev.zarr.zarrjava.core.GroupMetadata {

  static final int ZARR_FORMAT = 2;
  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public GroupMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat
  ) throws ZarrException {
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
  }

  public GroupMetadata() throws ZarrException {
    this(ZARR_FORMAT);
  }

}
