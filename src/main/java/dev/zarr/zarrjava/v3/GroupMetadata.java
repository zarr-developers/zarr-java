package dev.zarr.zarrjava.v3;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

public final class GroupMetadata extends dev.zarr.zarrjava.core.GroupMetadata {

    static final String NODE_TYPE = "group";
    static final int ZARR_FORMAT = 3;
    @JsonProperty("zarr_format")
    public final int zarrFormat = ZARR_FORMAT;
    @JsonProperty("node_type")
    public final String nodeType = "group";
    @JsonProperty("consolidated_metadata")
    public final Object consolidatedMetadata = null;

    @Nullable
    public final Attributes attributes;

    /**
     * Members of the metadata document that zarr-java does not know about. The Zarr v3 specification
     * requires that these are ignored when they declare {@code "must_understand": false}, and that
     * they are rejected otherwise. They are kept here so that rewriting the metadata does not drop
     * extensions written by another implementation.
     */
    private final Map<String, Object> extraFields;

    public GroupMetadata(@Nullable Attributes attributes) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, attributes, null);
    }

    public GroupMetadata(
            @Nullable Attributes attributes, @Nullable Map<String, Object> extraFields
    ) throws ZarrException {
        this(ZARR_FORMAT, NODE_TYPE, attributes, extraFields);
    }

    public GroupMetadata(int zarrFormat, String nodeType, @Nullable Attributes attributes)
            throws ZarrException {
        this(zarrFormat, nodeType, attributes, null);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public GroupMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "node_type", required = true) String nodeType,
            @Nullable @JsonProperty(value = "attributes") Attributes attributes,
            @Nullable @JsonAnySetter Map<String, Object> extraFields
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
        this.extraFields = ExtraFields.validatedGroupFields(extraFields);
    }

    public static GroupMetadata defaultValue() {
        try {
            return new GroupMetadata(ZARR_FORMAT, NODE_TYPE, new Attributes());
        } catch (ZarrException e) {
            // This should never happen with default values
            throw new IllegalStateException(
                    "Failed to create default GroupMetadata - this indicates a programming error", e);
        }
    }

    /**
     * The members of the metadata document that zarr-java does not know about, but that declared
     * {@code "must_understand": false} and could therefore be ignored. They are written back out
     * unchanged, so that extensions written by another implementation survive a metadata rewrite.
     *
     * @return the extra fields, never {@code null}
     */
    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return extraFields;
    }

    @Override
    public @Nonnull Attributes attributes() throws ZarrException {
        if (attributes == null) {
            throw new ZarrException("Group attributes have not been set.");
        }
        return attributes;
    }
}
