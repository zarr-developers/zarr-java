package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrSceneV06Test extends OmeZarrBaseTest {

    private static final Path V06_SCENE_REGISTRATION =
            TESTDATA.resolve("ome/v0.6/examples/user_stories/image_registration_3d.zarr");
    private static final Path V06_SCENE_EXAMPLE1 =
            TESTDATA.resolve("ome/v0.6_scene/example1_instrument_registration.zarr");
    private static final Path V06_SCENE_EXAMPLE2 =
            TESTDATA.resolve("ome/v0.6_scene/example2_multi_instrument_chain.zarr");

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return new FilesystemStore(TESTDATA.resolve("ome/v0.6/examples/2d/basic/scale_multiscale.zarr")).resolve();
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class;
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
        dev.zarr.zarrjava.experimental.ome.v0_6.Scene scene = dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(sceneHandle);

        assertNotNull(scene.getSceneMetadata());
        assertNotNull(scene.getSceneMetadata().coordinateTransformations);
        assertFalse(scene.getSceneMetadata().coordinateTransformations.isEmpty());

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation top =
                scene.getSceneMetadata().coordinateTransformations.get(0);
        assertEquals("bijection", top.getType());
        assertEquals("JRC2018F#physical", top.getInput());
        assertEquals("FCWB#physical", top.getOutput());

        assertTrue(scene.listImageNodes().contains("FCWB"));
        assertTrue(scene.listImageNodes().contains("JRC2018F"));

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage image = scene.openImageNode("FCWB");
        assertEquals(2, image.getScaleLevelCount());
        assertEquals(Arrays.asList("z", "y", "x"), image.getAxisNames());

        dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
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

        dev.zarr.zarrjava.experimental.ome.metadata.Axis y = new dev.zarr.zarrjava.experimental.ome.metadata.Axis("y", "space", "micrometer");
        dev.zarr.zarrjava.experimental.ome.metadata.Axis x = new dev.zarr.zarrjava.experimental.ome.metadata.Axis("x", "space", "micrometer");

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata sceneMetadata =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.SceneMetadata(
                        Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.TranslationCoordinateTransformation(
                                "imageA#physical", ".#world", "imageA to world", Arrays.asList(1.0, 2.0), null)),
                        Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem(
                                "world", Arrays.asList(y, x)))
                );

        dev.zarr.zarrjava.experimental.ome.v0_6.Scene.createScene(root, sceneMetadata);

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry imageAEntry =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                        null,
                        Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset(
                                "s0",
                                Collections.singletonList(
                                        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation.scale(
                                                Arrays.asList(1.0, 1.0), "s0", "physical")))),
                        null,
                        Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem(
                                "physical", Arrays.asList(y, x))),
                        "multiscales",
                        null,
                        null);

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(root.resolve("imageA"), imageAEntry);
        dev.zarr.zarrjava.v3.Array.create(
                root.resolve("imageA").resolve("s0"),
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16)
                        .withChunkShape(8, 8)
                        .withDataType(dev.zarr.zarrjava.v3.DataType.UINT16)
                        .build());

        dev.zarr.zarrjava.experimental.ome.v0_6.Scene reopened = dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(root);
        assertEquals(Collections.singletonList("imageA"), reopened.listImageNodes());
        assertEquals("world", reopened.getSceneMetadata().coordinateSystems.get(0).name);
        assertEquals("imageA#physical", reopened.getSceneMetadata().coordinateTransformations.get(0).getInput());

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage imageA = reopened.openImageNode("imageA");
        assertEquals(1, imageA.getScaleLevelCount());
        assertArrayEquals(new long[]{16, 16}, imageA.openScaleLevel(0).metadata().shape);

        dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph graph = reopened.getCoordinateTransformationGraph();
        assertEquals(2, graph.nodes.size());
        assertEquals(1, graph.edges.size());
        assertEquals("coordinateTransformations/lens", dev.zarr.zarrjava.experimental.ome.v0_6.Scene.normalizeCoordinateTransformPath("./coordinateTransformations/lens"));

        reopened.createCoordinateTransformationsGroup();
        assertTrue(root.resolve("coordinateTransformations").resolve(dev.zarr.zarrjava.core.Node.ZARR_JSON).exists());
    }

    @Test
    void openSceneExample1AffinePathBetweenInstruments() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_6.Scene scene =
                dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(new FilesystemStore(V06_SCENE_EXAMPLE1).resolve());
        assertEquals(Arrays.asList("sampleA_instrument1", "sampleA_instrument2"), scene.listImageNodes());
        assertEquals(1, scene.getSceneMetadata().coordinateTransformations.size());
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation ct =
                scene.getSceneMetadata().coordinateTransformations.get(0);
        assertEquals("affine", ct.getType());
        assertEquals("sampleA_instrument2#physical_instrument2", ct.getInput());
        assertEquals("sampleA_instrument1#physical_instrument1", ct.getOutput());
        assertTrue(ct instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation);
        assertEquals("coordinateTransformations/sampleA_instrument2-to-instrument1",
                ((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) ct).path);

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage i1 = scene.openImageNode("sampleA_instrument1");
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage i2 = scene.openImageNode("sampleA_instrument2");
        assertEquals(Arrays.asList("z", "y", "x"), i1.getAxisNames());
        assertEquals(Arrays.asList("z", "y", "x"), i2.getAxisNames());

        dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
        assertEquals(2, graph.nodes.size());
        assertEquals(1, graph.edges.size());
        assertTrue(graph.warnings.isEmpty());
    }

    @Test
    void openSceneExample2TwoAffineLinksViaInstrument2() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_6.Scene scene =
                dev.zarr.zarrjava.experimental.ome.v0_6.Scene.openScene(new FilesystemStore(V06_SCENE_EXAMPLE2).resolve());
        assertEquals(new HashSet<>(Arrays.asList("instrument1", "instrument2", "instrument3")), new HashSet<>(scene.listImageNodes()));
        assertEquals(2, scene.getSceneMetadata().coordinateTransformations.size());

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation t0 =
                scene.getSceneMetadata().coordinateTransformations.get(0);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation t1 =
                scene.getSceneMetadata().coordinateTransformations.get(1);
        assertEquals("affine", t0.getType());
        assertEquals("affine", t1.getType());
        assertEquals("instrument1#physical", t0.getInput());
        assertEquals("instrument2#physical", t0.getOutput());
        assertEquals("instrument3#physical", t1.getInput());
        assertEquals("instrument2#physical", t1.getOutput());
        assertTrue(t0 instanceof dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation);
        assertTrue(((dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.AffineCoordinateTransformation) t0).affine.size() > 0);

        dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
        assertEquals(3, graph.nodes.size());
        assertEquals(2, graph.edges.size());
        assertTrue(graph.warnings.isEmpty());
    }
}
