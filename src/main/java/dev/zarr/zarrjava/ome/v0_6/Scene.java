package dev.zarr.zarrjava.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ome.OmeV3Group;
import dev.zarr.zarrjava.ome.v0_6.metadata.SceneCoordinateTransformation;
import dev.zarr.zarrjava.ome.v0_6.metadata.SceneMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** OME-Zarr v0.6 scene root group (scene metadata). */
public final class Scene extends OmeV3Group {

    private final dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata;
    private final Map<String, MultiscaleImage> imageNodes;

    private Scene(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata,
            @Nonnull Map<String, MultiscaleImage> imageNodes
    ) throws IOException {
        super(storeHandle, groupMetadata);
        this.omeMetadata = omeMetadata;
        this.imageNodes = Collections.unmodifiableMap(new LinkedHashMap<>(imageNodes));
    }

    public static Scene open(@Nonnull StoreHandle storeHandle) throws IOException {
        try {
            return openScene(storeHandle);
        } catch (ZarrException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public static Scene openScene(@Nonnull StoreHandle storeHandle) throws IOException, ZarrException {
        Group group = Group.open(storeHandle);
        dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata.class);
        if (!omeMetadata.version.startsWith("0.6")) {
            throw new ZarrException(
                    "Expected OME-Zarr version '0.6', got '" + omeMetadata.version + "' at " + storeHandle);
        }
        if (omeMetadata.scene == null) {
            throw new ZarrException("No 'scene' found in ome metadata at " + storeHandle);
        }

        Map<String, MultiscaleImage> discovered = new LinkedHashMap<>();
        for (String child : asList(storeHandle.listChildren())) {
            try {
                MultiscaleImage image = MultiscaleImage.openMultiscaleImage(storeHandle.resolve(child));
                if (image.getRawOmeMetadata().multiscales != null && !image.getRawOmeMetadata().multiscales.isEmpty()) {
                    discovered.put(child, image);
                }
            } catch (Exception ignored) {
                // child is not a v0.6 multiscale image, ignore
            }
        }
        return new Scene(storeHandle, group.metadata, omeMetadata, discovered);
    }

    public static Scene createScene(
            @Nonnull StoreHandle storeHandle,
            @Nonnull SceneMetadata sceneMetadata
    ) throws IOException, ZarrException {
        dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata omeMetadata =
                new dev.zarr.zarrjava.ome.v0_6.metadata.OmeMetadata("0.6", null, null, sceneMetadata);
        Group group = Group.create(storeHandle, omeAttributes(omeMetadata));
        return new Scene(storeHandle, group.metadata, omeMetadata, new LinkedHashMap<String, MultiscaleImage>());
    }

    public static Scene create(
            @Nonnull StoreHandle storeHandle,
            @Nonnull SceneMetadata sceneMetadata
    ) throws IOException, ZarrException {
        return createScene(storeHandle, sceneMetadata);
    }

    public SceneMetadata getSceneMetadata() {
        return omeMetadata.scene;
    }

    public List<String> listImageNodes() {
        return new ArrayList<>(imageNodes.keySet());
    }

    public MultiscaleImage openImageNode(String path) throws IOException, ZarrException {
        if (path == null || path.isEmpty()) {
            throw new ZarrException("Image node path must be non-empty");
        }
        MultiscaleImage discovered = imageNodes.get(path);
        if (discovered != null) {
            return discovered;
        }
        MultiscaleImage opened = MultiscaleImage.openMultiscaleImage(storeHandle.resolve(path));
        if (opened.getRawOmeMetadata().multiscales == null || opened.getRawOmeMetadata().multiscales.isEmpty()) {
            throw new ZarrException("No multiscales metadata found at image node path '" + path + "'");
        }
        return opened;
    }

    public SceneTransformationGraph getCoordinateTransformationGraph() {
        List<SceneTransformationGraph.Node> nodes = new ArrayList<>();
        List<SceneTransformationGraph.Edge> edges = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        SceneReferenceResolver resolver = new SceneReferenceResolver(getSceneMetadata(), imageNodes);
        for (SceneReferenceResolver.ResolvedCoordinateSystem resolved : resolver.list()) {
            List<String> axisNames = new ArrayList<>();
            if (resolved.coordinateSystem.axes != null) {
                for (dev.zarr.zarrjava.ome.metadata.Axis axis : resolved.coordinateSystem.axes) {
                    axisNames.add(axis.name);
                }
            }
            nodes.add(new SceneTransformationGraph.Node(
                    resolved.id,
                    resolved.groupPath,
                    resolved.coordinateSystem.name,
                    axisNames));
        }

        if (omeMetadata.scene != null && omeMetadata.scene.coordinateTransformations != null) {
            for (SceneCoordinateTransformation transformation : omeMetadata.scene.coordinateTransformations) {
                addTransformationEdges(transformation, resolver, edges, warnings, null);
            }
        }

        return new SceneTransformationGraph(nodes, edges, warnings);
    }

    public Group createCoordinateTransformationsGroup() throws IOException, ZarrException {
        return createGroup("coordinateTransformations");
    }

    public static String normalizeCoordinateTransformPath(String path) {
        if (path == null) {
            return null;
        }
        String normalized = path.trim().replace('\\', '/');
        while (normalized.startsWith("./")) {
            normalized = normalized.substring(2);
        }
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    private static void addTransformationEdges(
            SceneCoordinateTransformation transformation,
            SceneReferenceResolver resolver,
            List<SceneTransformationGraph.Edge> edges,
            List<String> warnings,
            String inheritedName
    ) {
        if (transformation == null) {
            return;
        }
        String edgeName = transformation.name != null ? transformation.name : inheritedName;
        SceneReferenceResolver.ResolvedCoordinateSystem input = resolver.resolve(transformation.input);
        SceneReferenceResolver.ResolvedCoordinateSystem output = resolver.resolve(transformation.output);

        if (transformation.input != null && input == null) {
            warnings.add("Unresolved scene input coordinate system: " + transformation.input.canonicalId());
        }
        if (transformation.output != null && output == null) {
            warnings.add("Unresolved scene output coordinate system: " + transformation.output.canonicalId());
        }

        edges.add(new SceneTransformationGraph.Edge(
                edgeName,
                transformation.type,
                input != null ? input.id : null,
                output != null ? output.id : null,
                normalizeCoordinateTransformPath(transformation.path)));

        if (transformation.sequenceTransformations != null) {
            for (SceneCoordinateTransformation nested : transformation.sequenceTransformations) {
                addTransformationEdges(nested, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation.byDimensionTransformations != null) {
            for (SceneCoordinateTransformation.ByDimensionTransformation byDim : transformation.byDimensionTransformations) {
                addTransformationEdges(byDim.transformation, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation.transformation != null) {
            addTransformationEdges(transformation.transformation, resolver, edges, warnings, edgeName);
        }
        if (transformation.forward != null) {
            addTransformationEdges(transformation.forward, resolver, edges, warnings, edgeName);
        }
        if (transformation.inverse != null) {
            addTransformationEdges(transformation.inverse, resolver, edges, warnings, edgeName);
        }
    }

    private static List<String> asList(java.util.stream.Stream<String> stream) {
        try {
            return stream.collect(java.util.stream.Collectors.toList());
        } finally {
            stream.close();
        }
    }
}
