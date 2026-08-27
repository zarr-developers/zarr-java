package dev.zarr.zarrjava.v3;

import dev.zarr.zarrjava.ZarrException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handling of unknown ("extra") members of a Zarr v3 metadata document.
 *
 * <p>The Zarr v3 specification allows a writer to add members that a reader may not know about. Such
 * a member has to be a JSON object carrying {@code "must_understand": false}, which declares that a
 * reader that does not know the member may safely ignore it. Any other unknown member has to be
 * rejected, because it may change how the array is to be interpreted.
 *
 * <p>These semantics match zarr-python 3.1.6, see {@code zarr/core/metadata/v3.py}.
 */
final class ExtraFields {

    static final String MUST_UNDERSTAND = "must_understand";

    /**
     * Members of a v3 array metadata document that zarr-java knows about. Anything else read from a
     * {@code zarr.json} array document is an extra field.
     */
    static final Set<String> ARRAY_METADATA_KEYS = unmodifiableSetOf(
            "zarr_format", "node_type", "shape", "data_type", "chunk_grid", "chunk_key_encoding",
            "fill_value", "codecs", "attributes", "dimension_names", "storage_transformers"
    );

    /**
     * Members of a v3 group metadata document that zarr-java knows about. Anything else read from a
     * {@code zarr.json} group document is an extra field.
     */
    static final Set<String> GROUP_METADATA_KEYS = unmodifiableSetOf(
            "zarr_format", "node_type", "attributes", "consolidated_metadata"
    );

    private ExtraFields() {
    }

    private static Set<String> unmodifiableSetOf(String... keys) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(keys)));
    }

    /**
     * Whether an unknown metadata member may be ignored, i.e. whether it is a JSON object with a
     * {@code must_understand} member that is set to {@code false}.
     */
    static boolean isIgnorable(@Nullable Object value) {
        return value instanceof Map
                && Boolean.FALSE.equals(((Map<?, ?>) value).get(MUST_UNDERSTAND));
    }

    /**
     * Validates unknown members of a metadata document and returns them so that they can be written
     * back out unchanged.
     *
     * @param extraFields  the unknown members, may be {@code null}
     * @param reservedKeys the members that the metadata document defines itself, either
     *                     {@link #ARRAY_METADATA_KEYS} or {@link #GROUP_METADATA_KEYS}
     * @return the extra fields, never {@code null}
     * @throws ZarrException if a member collides with a reserved key, or if a member may not be
     *                       ignored because it is not a JSON object carrying
     *                       {@code "must_understand": false}
     */
    static Map<String, Object> validated(
            @Nullable Map<String, Object> extraFields, Set<String> reservedKeys
    ) throws ZarrException {
        if (extraFields == null || extraFields.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<String> reservedCollisions = new ArrayList<>();
        final List<String> notIgnorable = new ArrayList<>();
        for (Map.Entry<String, Object> entry : extraFields.entrySet()) {
            if (reservedKeys.contains(entry.getKey())) {
                reservedCollisions.add(entry.getKey());
            } else if (!isIgnorable(entry.getValue())) {
                notIgnorable.add(entry.getKey());
            }
        }
        if (!reservedCollisions.isEmpty()) {
            Collections.sort(reservedCollisions);
            throw new ZarrException(
                    "Invalid extra fields. The following keys: " + reservedCollisions + " are invalid " +
                            "because they collide with keys reserved for use by the metadata document.");
        }
        if (!notIgnorable.isEmpty()) {
            Collections.sort(notIgnorable);
            throw new ZarrException(
                    "Got a Zarr v3 metadata document with the following disallowed extra fields: " +
                            notIgnorable + ". Extra fields are not allowed unless they are a JSON object " +
                            "with a \"" + MUST_UNDERSTAND + "\" key which is assigned the value `false`.");
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(extraFields));
    }
}
