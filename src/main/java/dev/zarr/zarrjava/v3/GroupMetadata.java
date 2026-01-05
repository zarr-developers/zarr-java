package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GroupMetadata extends dev.zarr.zarrjava.core.GroupMetadata {

    static final String NODE_TYPE = "group";
    static final int ZARR_FORMAT = 3;
    @JsonProperty("zarr_format")
    public final int zarrFormat = ZARR_FORMAT;
    @JsonProperty("node_type")
    public final String nodeType = "group";
    @JsonProperty("consolidated_metadata")
    public final String consolidatedMetadata = null;

    @Nullable
    public final Attributes attributes;

    public GroupMetadata(@Nullable Attributes attributes) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, attributes);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GroupMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @Nullable @JsonProperty(value = "attributes") Attributes attributes
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

    public static GroupMetadata defaultValue() {
        try {
            return new GroupMetadata(ZARR_FORMAT, NODE_TYPE, new Attributes());
        } catch (ZarrException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
    }

    @Override
    public @Nonnull Attributes attributes() throws ZarrException {
        if (attributes == null) {
            throw new ZarrException("Group attributes have not been set.");
        }
        return attributes;
    }
}
