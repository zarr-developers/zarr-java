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
 * @param <M> the concrete multiscales entry type
 */
public interface MultiscalesMetadataImage<M extends MultiscalesEntry> extends MultiscaleImage {

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
        M entry = getMultiscalesEntry(i);
        List<UnifiedSinglescaleNode> nodes = new ArrayList<>();
        for (dev.zarr.zarrjava.ome.metadata.Dataset dataset : entry.datasets) {
            nodes.add(new UnifiedSinglescaleNode(dataset.path, dataset.coordinateTransformations));
        }
        return new UnifiedMultiscaleNode(entry.name, entry.axes, nodes);
    }
}
