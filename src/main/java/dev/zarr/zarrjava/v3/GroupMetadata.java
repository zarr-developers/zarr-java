package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public final class GroupMetadata {

  static final String NODE_TYPE = "group";
  static final int ZARR_FORMAT = 3;
  @JsonProperty("zarr_format")
  public final int zarrFormat = 3;
  @JsonProperty("node_type")
  public final String nodeType = "group";

  @Nullable
  public final Map<String, Object> attributes;

  public GroupMetadata(@Nullable Map<String, Object> attributes) throws ZarrException {
    this(ZARR_FORMAT, NODE_TYPE, attributes);
  }

  @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
  public GroupMetadata(
      @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
      @JsonProperty(value = "node_type", required = true) String nodeType,
      @Nullable @JsonProperty(value = "attributes") Map<String, Object> attributes
  ) throws ZarrException {
    if (zarrFormat != this.zarrFormat) {
      throw new ZarrException(
          "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
    }
    if (!nodeType.equals(this.nodeType)) {
      throw new ZarrException(
          "Expected node type '" + this.nodeType + "', got '" + nodeType + "'.");
    }
    this.attributes = attributes;
  }

  public static GroupMetadata defaultValue() throws ZarrException {
    return new GroupMetadata(ZARR_FORMAT, NODE_TYPE, new HashMap<>());
  }
}
