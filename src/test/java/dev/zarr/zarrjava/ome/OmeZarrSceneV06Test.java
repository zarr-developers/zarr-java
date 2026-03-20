package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrSceneV06Test extends OmeZarrBaseTest {

    private static final Path V06_SCENE_REGISTRATION =
            TESTDATA.resolve("ome/v0.6/examples/user_stories/image_registration_3d.zarr");

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return new FilesystemStore(TESTDATA.resolve("ome/v0.6/examples/2d/basic/scale_multiscale.zarr")).resolve();
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.ome.v0_6.MultiscaleImage.class;
    }

    @Override
    int expectedScaleLevelCount() {
        return 3;
    }

    @Override
    long[] expectedLevel0Shape() {
        return new long[]{576, 720};
    }

    @Override
    java.util.List<String> expectedAxisNames() {
        return Arrays.asList("y", "x");
    }

    @Test
    void openSceneAndNavigateImages() throws Exception {
        StoreHandle sceneHandle = new FilesystemStore(V06_SCENE_REGISTRATION).resolve();
        dev.zarr.zarrjava.ome.v0_6.Scene scene = dev.zarr.zarrjava.ome.v0_6.Scene.openScene(sceneHandle);

        assertNotNull(scene.getSceneMetadata());
        assertNotNull(scene.getSceneMetadata().coordinateTransformations);
        assertFalse(scene.getSceneMetadata().coordinateTransformations.isEmpty());

        dev.zarr.zarrjava.ome.v0_6.metadata.SceneCoordinateTransformation top =
                scene.getSceneMetadata().coordinateTransformations.get(0);
        assertEquals("bijection", top.type);
        assertNotNull(top.input);
        assertEquals("JRC2018F", top.input.path);
        assertEquals("physical", top.input.name);
        assertNotNull(top.output);
        assertEquals("FCWB", top.output.path);
        assertEquals("physical", top.output.name);

        assertTrue(scene.listImageNodes().contains("FCWB"));
        assertTrue(scene.listImageNodes().contains("JRC2018F"));

        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage image = scene.openImageNode("FCWB");
        assertEquals(2, image.getScaleLevelCount());
        assertEquals(Arrays.asList("z", "y", "x"), image.getAxisNames());

        dev.zarr.zarrjava.ome.v0_6.SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
        assertFalse(graph.nodes.isEmpty());
        assertFalse(graph.edges.isEmpty());
        assertTrue(graph.warnings.isEmpty());
    }

    @Test
    void multiscaleOpenOnSceneRootGivesGuidance() throws Exception {
        StoreHandle sceneHandle = new FilesystemStore(V06_SCENE_REGISTRATION).resolve();
        Exception ex = assertThrows(dev.zarr.zarrjava.ZarrException.class, () -> MultiscaleImage.open(sceneHandle));
        assertTrue(ex.getMessage().contains("Scene.open"));
    }

    @Test
    void createAndReopenScene() throws Exception {
        Path out = TESTOUTPUT.resolve("ome_v06_scene_create");
        StoreHandle root = new FilesystemStore(out).resolve();

        dev.zarr.zarrjava.ome.metadata.Axis y = new dev.zarr.zarrjava.ome.metadata.Axis("y", "space", "micrometer");
        dev.zarr.zarrjava.ome.metadata.Axis x = new dev.zarr.zarrjava.ome.metadata.Axis("x", "space", "micrometer");

        dev.zarr.zarrjava.ome.v0_6.metadata.SceneMetadata sceneMetadata =
                new dev.zarr.zarrjava.ome.v0_6.metadata.SceneMetadata(
                        Collections.singletonList(new dev.zarr.zarrjava.ome.v0_6.metadata.SceneCoordinateTransformation(
                                "translation",
                                new dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystemRef("imageA", "physical"),
                                new dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystemRef(null, "world"),
                                "imageA to world",
                                null,
                                Arrays.asList(1.0, 2.0),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                        )),
                        Collections.singletonList(new dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem(
                                "world", Arrays.asList(y, x)))
                );

        dev.zarr.zarrjava.ome.v0_6.Scene.createScene(root, sceneMetadata);

        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry imageAEntry =
                new dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry(
                        null,
                        Collections.singletonList(new dev.zarr.zarrjava.ome.v0_6.metadata.Dataset(
                                "s0",
                                Collections.singletonList(
                                        dev.zarr.zarrjava.ome.v0_6.metadata.transform.CoordinateTransformation.scale(
                                                Arrays.asList(1.0, 1.0), "s0", "physical")))),
                        null,
                        Collections.singletonList(new dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem(
                                "physical", Arrays.asList(y, x))),
                        "multiscales",
                        null,
                        null);

        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage.create(root.resolve("imageA"), imageAEntry);
        dev.zarr.zarrjava.v3.Array.create(
                root.resolve("imageA").resolve("s0"),
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16)
                        .withChunkShape(8, 8)
                        .withDataType(dev.zarr.zarrjava.v3.DataType.UINT16)
                        .build());

        dev.zarr.zarrjava.ome.v0_6.Scene reopened = dev.zarr.zarrjava.ome.v0_6.Scene.openScene(root);
        assertEquals(Collections.singletonList("imageA"), reopened.listImageNodes());
        assertEquals("world", reopened.getSceneMetadata().coordinateSystems.get(0).name);
        assertEquals("imageA", reopened.getSceneMetadata().coordinateTransformations.get(0).input.path);

        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage imageA = reopened.openImageNode("imageA");
        assertEquals(1, imageA.getScaleLevelCount());
        assertArrayEquals(new long[]{16, 16}, imageA.openScaleLevel(0).metadata().shape);

        dev.zarr.zarrjava.ome.v0_6.SceneTransformationGraph graph = reopened.getCoordinateTransformationGraph();
        assertEquals(2, graph.nodes.size());
        assertEquals(1, graph.edges.size());
        assertEquals("coordinateTransformations/lens", dev.zarr.zarrjava.ome.v0_6.Scene.normalizeCoordinateTransformPath("./coordinateTransformations/lens"));

        reopened.createCoordinateTransformationsGroup();
        assertTrue(root.resolve("coordinateTransformations").resolve(dev.zarr.zarrjava.core.Node.ZARR_JSON).exists());
    }
}
