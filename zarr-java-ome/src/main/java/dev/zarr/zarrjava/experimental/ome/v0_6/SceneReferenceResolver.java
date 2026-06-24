package dev.zarr.zarrjava.experimental.ome.v0_6;

import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class SceneReferenceResolver {

    private final Map<String, ResolvedCoordinateSystem> index = new LinkedHashMap<>();

    SceneReferenceResolver(SceneMetadata sceneMetadata, Map<String, MultiscaleImage> imageNodes) {
        if (sceneMetadata != null && sceneMetadata.coordinateSystems != null) {
            for (CoordinateSystem coordinateSystem : sceneMetadata.coordinateSystems) {
                addCoordinateSystem(".", coordinateSystem);
            }
        }
        for (Map.Entry<String, MultiscaleImage> entry : imageNodes.entrySet()) {
            String path = entry.getKey();
            MultiscaleImage image = entry.getValue();
            try {
                int count = image.getRawOmeMetadata().multiscales == null ? 0 : image.getRawOmeMetadata().multiscales.size();
                for (int i = 0; i < count; i++) {
                    MultiscalesEntry multiscale = image.getMultiscalesEntry(i);
                    if (multiscale.coordinateSystems == null) {
                        continue;
                    }
                    for (CoordinateSystem coordinateSystem : multiscale.coordinateSystems) {
                        addCoordinateSystem(path, coordinateSystem);
                    }
                }
            } catch (Exception ignored) {
                // keep permissive parsing/open behavior
            }
        }
    }

    @Nullable
    ResolvedCoordinateSystem resolve(@Nullable String reference) {
        if (reference == null || reference.isEmpty()) {
            return null;
        }
        if (!reference.contains("#")) {
            return null;
        }
        return index.get(reference);
    }

    List<ResolvedCoordinateSystem> list() {
        return new ArrayList<>(index.values());
    }

    private void addCoordinateSystem(String groupPath, CoordinateSystem coordinateSystem) {
        if (coordinateSystem == null || coordinateSystem.name == null) {
            return;
        }
        String canonicalPath = groupPath == null || groupPath.isEmpty() ? "." : groupPath;
        String id = canonicalPath + "#" + coordinateSystem.name;
        index.put(id, new ResolvedCoordinateSystem(id, canonicalPath, coordinateSystem));
    }

    static final class ResolvedCoordinateSystem {
        final String id;
        final String groupPath;
        final CoordinateSystem coordinateSystem;

        ResolvedCoordinateSystem(String id, String groupPath, CoordinateSystem coordinateSystem) {
            this.id = id;
            this.groupPath = groupPath;
            this.coordinateSystem = coordinateSystem;
        }
    }
}
