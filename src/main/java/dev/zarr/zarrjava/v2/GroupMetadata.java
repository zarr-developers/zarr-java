package dev.zarr.zarrjava.v2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;

public final class GroupMetadata extends dev.zarr.zarrjava.core.GroupMetadata {

  static final int ZARR_FORMAT = 2;
  @JsonProperty("zarr_format")
  public final int zarrFormat = ZARR_FORMAT;

  @Nullable
  @JsonIgnore
  public Attributes attributes;

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public GroupMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
      @JsonProperty(value = "attributes", required = false) @Nullable Attributes attributes
  ) throws ZarrException {
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
    this.attributes = attributes;
  }

  public GroupMetadata() throws ZarrException {
    this(ZARR_FORMAT, null);
  }

  @Override
  public @Nonnull Attributes attributes() throws ZarrException {
    return attributes;
  }
}
