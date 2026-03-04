package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ome.metadata.Axis;

import javax.annotation.Nullable;
import java.util.List;

/** A multiscale image node, using v1.0 RFC-8 terminology ("nodes" not "datasets"). */
public final class UnifiedMultiscaleNode {

    @Nullable
    public final String name;
    public final List<Axis> axes;
    public final List<UnifiedSinglescaleNode> nodes;

    public UnifiedMultiscaleNode(@Nullable String name, List<Axis> axes, List<UnifiedSinglescaleNode> nodes) {
        this.name = name;
        this.axes = axes;
        this.nodes = nodes;
    }
}
