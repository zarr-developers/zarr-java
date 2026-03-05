package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Extension of {@link MultiscaleImage} that provides typed access to OME-Zarr multiscales metadata
 * and supports creating new scale levels.
 *
 * @param <M> the concrete multiscales entry type (may be {@link MultiscalesEntry} or a version-specific subtype)
 */
public interface MultiscalesMetadataImage<M> extends MultiscaleImage {

    /**
     * Returns the raw multiscales entry at index {@code i}.
     */
    M getMultiscalesEntry(int i) throws ZarrException;

    /**
     * Creates a new scale level array at {@code path} with the given metadata and coordinate transformations,
     * then registers it in the multiscales metadata.
     */
    void createScaleLevel(
            String path,
            dev.zarr.zarrjava.core.ArrayMetadata arrayMetadata,
            List<CoordinateTransformation> coordinateTransformations
    ) throws IOException, ZarrException;

    @Override
    default UnifiedMultiscaleNode getMultiscaleNode(int i) throws ZarrException {
        Object entry = getMultiscalesEntry(i);
        if (!(entry instanceof MultiscalesEntry)) {
            throw new ZarrException(
                    "getMultiscaleNode() not supported for entry type " + entry.getClass().getName()
                            + "; override getMultiscaleNode() in your MultiscalesMetadataImage implementation.");
        }
        MultiscalesEntry mse = (MultiscalesEntry) entry;
        List<UnifiedSinglescaleNode> nodes = new ArrayList<>();
        for (dev.zarr.zarrjava.ome.metadata.Dataset dataset : mse.datasets) {
            nodes.add(new UnifiedSinglescaleNode(dataset.path, dataset.coordinateTransformations));
        }
        return new UnifiedMultiscaleNode(mse.name, mse.axes, nodes);
    }
}
