package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.experimental.ome.v0_6.Scene;
import dev.zarr.zarrjava.experimental.ome.v0_6.SceneTransformationGraph;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads real OME-Zarr v0.6 datasets with chunk data over HTTP, from the RFC-5 test data bucket of
 * the University of Münster (NFDI4BioImage). The expected pixel sums were cross-checked with
 * zarr-python.
 */
public class OmeZarrV06RealDataTest {

    private static final HttpStore STORE = new HttpStore("https://radosgw.public.os.wwu.de/rfc5-transform-test-data");

    private static StoreHandle handle(String path) {
        return STORE.resolve(path.split("/"));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "cells3d.ome.zarr",
            "cells3d_scene_2d_to_3d.zarr",
            "cells3d_scene_2d_to_3d.zarr/cells3d",
            "cells3d_scene_2d_to_3d.zarr/cells3d_slice",
            "cells3d_scene_2d_to_3d_tilt.zarr",
            "logan_shepp_affine_45_degree.ome.zarr",
            "logan_shepp_displacements.ome.zarr",
            "logan_shepp_displacements.ome.zarr/coordinateTransformations/sine_wave_displacement_field",
            "logan_shepp_rotation_30_anticlockwise.ome.zarr",
            "logan_shepp_rotation_30_clockwise.ome.zarr",
            "logan_shepp_rotation_30_degree_anticlockwise_around_center.ome.zarr",
            "logan_shepp_translation_x.ome.zarr",
            "logan_shepp_translation_y.ome.zarr",
            "multiview_lightsheet_scene.zarr",
            "multiview_lightsheet_scene.zarr/img1",
            "P2A_B6_M2_Scene_2.ome.zarr",
            "P2A_B6_M2_Scene_2.ome.zarr/CBCT",
            "P2A_B6_M2_Scene_2.ome.zarr/GNI",
            "scene_with_displacements.zarr",
            "scene_with_displacements.zarr/HE",
            "scene_with_displacements.zarr/coordinateTransformations/dfield_HE2GNI",
            "test_ngff_plate.zarr",
            "test_ngff_plate.zarr/A/2",
            "test_ngff_plate.zarr/A/2/0",
            "tiles_with_overlap.ome.zarr",
            "tiles_with_overlap.ome.zarr/img1",
    })
    void roundTripMetadata(String path) throws Exception {
        JsonNode zarrJson = new ObjectMapper().readTree(handle(path).resolve("zarr.json").readNonNull().array());
        OmeZarrV06RoundTripTest.assertRoundTrip(zarrJson.get("attributes").get("ome"));
    }

    @Test
    void readMultiscaleImage() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(handle("cells3d.ome.zarr"));
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class, image);
        assertEquals(Arrays.asList("c", "z", "y", "x"), image.getAxisNames());
        assertEquals(4, image.getScaleLevelCount());
        dev.zarr.zarrjava.core.Array s0 = image.openScaleLevel(0);
        assertArrayEquals(new long[]{2, 60, 256, 256}, s0.metadata().shape);
        assertBlock(s0, new long[]{2, 60, 64, 64}, 2303480630.0, 491520);
    }

    @Test
    void openSceneWithoutListing() throws Exception {
        Scene scene = Scene.openScene(handle("tiles_with_overlap.ome.zarr"));
        assertEquals(Arrays.asList("img1", "img2", "img3", "img4"), scene.listImageNodes());
        SceneTransformationGraph graph = scene.getCoordinateTransformationGraph();
        assertEquals(Collections.emptyList(), graph.warnings);
        assertEquals(4, graph.edges.size());

        MultiscaleImage tile = scene.openImageNode("img1");
        assertEquals(Arrays.asList("y", "x"), tile.getAxisNames());
        assertBlock(tile.openScaleLevel(0), new long[]{64, 64}, 67853.0, 4096);
    }

    @Test
    void openSceneAcrossModalities() throws Exception {
        Scene scene = Scene.openScene(handle("P2A_B6_M2_Scene_2.ome.zarr"));
        assertEquals(Arrays.asList("Dose", "LET", "GNI", "CBCT"), scene.listImageNodes());
        assertEquals(Collections.emptyList(), scene.getCoordinateTransformationGraph().warnings);
        assertArrayEquals(new long[]{4, 16702, 20343}, scene.openImageNode("GNI").openScaleLevel(0).metadata().shape);
        assertEquals(Arrays.asList("z", "y", "x"), scene.openImageNode("CBCT").getAxisNames());
    }

    @Test
    void openSceneWithDisplacementFields() throws Exception {
        Scene scene = Scene.openScene(handle("scene_with_displacements.zarr"));
        assertEquals(Arrays.asList("HE", "GNI", "NOK"), scene.listImageNodes());
        assertEquals(Collections.emptyList(), scene.getCoordinateTransformationGraph().warnings);
        assertBlock(scene.openImageNode("HE").openScaleLevel(0), new long[]{3, 64, 64}, 3133440.0, 12288);
    }

    @Test
    void readPlate() throws Exception {
        Plate plate = Plate.open(handle("test_ngff_plate.zarr"));
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.Plate.class, plate);
        Well well = plate.openWell("B/3");
        assertEquals(3, well.getWellMetadata().images.size());
        MultiscaleImage image = well.openImage("2");
        assertEquals(Arrays.asList("z", "y", "x"), image.getAxisNames());
        assertEquals(5, image.getScaleLevelCount());
        assertBlock(image.openScaleLevel(0), new long[]{10, 64, 64}, 408734.0, 40958);
    }

    private static void assertBlock(dev.zarr.zarrjava.core.Array array, long[] shape, double expectedSum,
                                    long expectedNonZero) throws Exception {
        ucar.ma2.Array data = array.read(new long[shape.length], shape);
        double sum = 0;
        long nonZero = 0;
        ucar.ma2.IndexIterator it = data.getIndexIterator();
        while (it.hasNext()) {
            double value = it.getDoubleNext();
            sum += value;
            if (value != 0) {
                nonZero++;
            }
        }
        assertEquals(expectedSum, sum);
        assertEquals(expectedNonZero, nonZero);
        assertTrue(nonZero > 0);
    }
}
