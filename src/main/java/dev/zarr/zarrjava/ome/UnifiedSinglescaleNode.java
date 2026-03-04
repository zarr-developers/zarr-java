package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;

import java.util.List;

/** A single scale level within a multiscale image, using v1.0 RFC-8 terminology. */
public final class UnifiedSinglescaleNode {

    public final String path;
    public final List<CoordinateTransformation> coordinateTransformations;

    public UnifiedSinglescaleNode(String path, List<CoordinateTransformation> coordinateTransformations) {
        this.path = path;
        this.coordinateTransformations = coordinateTransformations;
    }
}
