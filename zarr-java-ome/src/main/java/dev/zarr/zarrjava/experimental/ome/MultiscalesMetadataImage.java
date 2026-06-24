package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;

import java.io.IOException;
import java.util.List;

/**
 * Extension of {@link MultiscaleImage} that provides typed access to OME-Zarr multiscales metadata
 * and supports creating new scale levels.
 *
 * @param <M> the concrete multiscales entry type (may be {@link MultiscalesEntry} or a version-specific subtype)
 */
public interface MultiscalesMetadataImage<M> extends MultiscaleImage {

    /**
     * Returns the raw multiscales entry at index {@code i} — the version-specific type.
     */
    M getMultiscalesEntry(int i) throws ZarrException;

    /**
     * Creates a new scale level array at {@code path} with the given metadata and coordinate
     * transformations, then registers it in the multiscales metadata.
     */
    void createScaleLevel(
            String path,
            dev.zarr.zarrjava.core.ArrayMetadata arrayMetadata,
            List<CoordinateTransformation> coordinateTransformations
    ) throws IOException, ZarrException;

    /**
     * Default implementation: casts the version-specific entry to the shared {@link MultiscalesEntry}.
     * Versions whose entry type does not extend {@link MultiscalesEntry} (e.g., v0.6) must
     * override {@link #getMultiscaleNode(int)} directly.
     */
    @Override
    default MultiscalesEntry getMultiscaleNode(int i) throws ZarrException {
        Object entry = getMultiscalesEntry(i);
        if (!(entry instanceof MultiscalesEntry)) {
            throw new ZarrException(
                    "getMultiscaleNode() not supported for entry type " + entry.getClass().getName()
                    + "; override getMultiscaleNode() in your implementation.");
        }
        return (MultiscalesEntry) entry;
    }
}
