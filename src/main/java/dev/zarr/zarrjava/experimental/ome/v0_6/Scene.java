package dev.zarr.zarrjava.experimental.ome.v0_6;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.OmeV3Group;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ByDimensionCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.BijectionCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.GenericCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.MapAxisCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.SequenceCoordinateTransformation;
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

    private final dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata omeMetadata;
    private final Map<String, MultiscaleImage> imageNodes;

    private Scene(
            @Nonnull StoreHandle storeHandle,
            @Nonnull GroupMetadata groupMetadata,
            @Nonnull dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata omeMetadata,
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
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata omeMetadata = readOmeAttribute(
                group.metadata.attributes, storeHandle, dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata.class);
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
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata omeMetadata =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata("0.6", null, null, sceneMetadata);
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
                for (dev.zarr.zarrjava.experimental.ome.metadata.Axis axis : resolved.coordinateSystem.axes) {
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
            for (CoordinateTransformation transformation : omeMetadata.scene.coordinateTransformations) {
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
            CoordinateTransformation transformation,
            SceneReferenceResolver resolver,
            List<SceneTransformationGraph.Edge> edges,
            List<String> warnings,
            String inheritedName
    ) {
        if (transformation == null) {
            return;
        }
        String edgeName = transformation.getName() != null ? transformation.getName() : inheritedName;
        SceneReferenceResolver.ResolvedCoordinateSystem input = resolver.resolve(transformation.getInput());
        SceneReferenceResolver.ResolvedCoordinateSystem output = resolver.resolve(transformation.getOutput());

        if (transformation.getInput() != null && input == null) {
            warnings.add("Unresolved scene input coordinate system: " + transformation.getInput());
        }
        if (transformation.getOutput() != null && output == null) {
            warnings.add("Unresolved scene output coordinate system: " + transformation.getOutput());
        }

        edges.add(new SceneTransformationGraph.Edge(
                edgeName,
                transformation.getType(),
                input != null ? input.id : null,
                output != null ? output.id : null,
                normalizeCoordinateTransformPath(extractPath(transformation))));

        if (transformation instanceof SequenceCoordinateTransformation) {
            SequenceCoordinateTransformation seq = (SequenceCoordinateTransformation) transformation;
            if (seq.transformations != null) for (CoordinateTransformation nested : seq.transformations) {
                addTransformationEdges(nested, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation instanceof ByDimensionCoordinateTransformation) {
            ByDimensionCoordinateTransformation byDim = (ByDimensionCoordinateTransformation) transformation;
            if (byDim.transformations != null) for (ByDimensionCoordinateTransformation.ByDimensionTransformation item : byDim.transformations) {
                addTransformationEdges(item.transformation, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation instanceof MapAxisCoordinateTransformation) {
            MapAxisCoordinateTransformation mapAxis = (MapAxisCoordinateTransformation) transformation;
            if (mapAxis.transformation != null) {
                addTransformationEdges(mapAxis.transformation, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation instanceof BijectionCoordinateTransformation) {
            BijectionCoordinateTransformation b = (BijectionCoordinateTransformation) transformation;
            if (b.forward != null) {
                addTransformationEdges(b.forward, resolver, edges, warnings, edgeName);
            }
            if (b.inverse != null) {
                addTransformationEdges(b.inverse, resolver, edges, warnings, edgeName);
            }
        }
        if (transformation instanceof GenericCoordinateTransformation) {
            GenericCoordinateTransformation generic = (GenericCoordinateTransformation) transformation;
            Object nested = generic.raw.get("transformations");
            if (nested instanceof List) {
                for (Object item : (List<?>) nested) {
                    if (item instanceof CoordinateTransformation) {
                        addTransformationEdges((CoordinateTransformation) item, resolver, edges, warnings, edgeName);
                    } else if (item instanceof ByDimensionCoordinateTransformation.ByDimensionTransformation) {
                        addTransformationEdges(
                                ((ByDimensionCoordinateTransformation.ByDimensionTransformation) item).transformation,
                                resolver, edges, warnings, edgeName);
                    }
                }
            }
            Object inner = generic.raw.get("transformation");
            if (inner instanceof CoordinateTransformation) {
                addTransformationEdges((CoordinateTransformation) inner, resolver, edges, warnings, edgeName);
            }
            Object forward = generic.raw.get("forward");
            if (forward instanceof CoordinateTransformation) {
                addTransformationEdges((CoordinateTransformation) forward, resolver, edges, warnings, edgeName);
            }
            Object inverse = generic.raw.get("inverse");
            if (inverse instanceof CoordinateTransformation) {
                addTransformationEdges((CoordinateTransformation) inverse, resolver, edges, warnings, edgeName);
            }
        }
    }

    private static String extractPath(CoordinateTransformation transformation) {
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.IdentityCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.RotationCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.DisplacementsCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) {
            return ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinatesCoordinateTransformation) transformation).path;
        }
        if (transformation instanceof GenericCoordinateTransformation) {
            Object p = ((GenericCoordinateTransformation) transformation).raw.get("path");
            return p instanceof String ? (String) p : null;
        }
        return null;
    }

    private static List<String> asList(java.util.stream.Stream<String> stream) {
        try {
            return stream.collect(java.util.stream.Collectors.toList());
        } finally {
            stream.close();
        }
    }
}
