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

    /**
     * An optional cache of the metadata of all descendants of this group, or null if this group has
     * not been consolidated. See {@link ConsolidatedMetadata} and {@link Group#consolidateMetadata()}.
     */
    @Nullable
    @JsonProperty("consolidated_metadata")
    public final ConsolidatedMetadata consolidatedMetadata;

    @Nullable
    public final Attributes attributes;

    public GroupMetadata(@Nullable Attributes attributes) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, attributes, null);
    }

    public GroupMetadata(
            @Nullable Attributes attributes,
            @Nullable ConsolidatedMetadata consolidatedMetadata
    ) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, attributes, consolidatedMetadata);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GroupMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @Nullable @JsonProperty(value = "attributes") Attributes attributes,
            @Nullable @JsonProperty(value = "consolidated_metadata") ConsolidatedMetadata consolidatedMetadata
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
        this.consolidatedMetadata = consolidatedMetadata;
    }

    public static GroupMetadata defaultValue() {
        try {
            return new GroupMetadata(ZARR_FORMAT, NODE_TYPE, new Attributes(), null);
        } catch (ZarrException e) {
            // This should never happen with default values
            throw new IllegalStateException(
                    "Failed to create default GroupMetadata - this indicates a programming error", e);
        }
    }

    /**
     * Returns a copy of this metadata with a different consolidated metadata cache, or without one if
     * {@code newConsolidatedMetadata} is null.
     */
    public GroupMetadata withConsolidatedMetadata(@Nullable ConsolidatedMetadata newConsolidatedMetadata) {
        try {
            return new GroupMetadata(zarrFormat, nodeType, attributes, newConsolidatedMetadata);
        } catch (ZarrException e) {
            // This should never happen, the format and node type are copied from a valid instance
            throw new IllegalStateException(
                    "Failed to copy GroupMetadata - this indicates a programming error", e);
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
