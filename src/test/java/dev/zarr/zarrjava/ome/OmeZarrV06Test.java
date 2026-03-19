package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrV06Test extends OmeZarrBaseTest {

    private static final java.nio.file.Path V06_2D =
            TESTDATA.resolve("ome/v0.6/examples/2d/basic/scale_multiscale.zarr");
    private static final java.nio.file.Path V06_3D =
            TESTDATA.resolve("ome/v0.6/examples/3d/basic/scale_multiscale.zarr");

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return storeHandle(V06_2D);
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.ome.v0_6.MultiscaleImage.class;
    }

    @Override
    int expectedScaleLevelCount() { return 3; }

    @Override
    long[] expectedLevel0Shape() { return new long[]{576, 720}; }

    @Override
    List<String> expectedAxisNames() {
        return Arrays.asList("y", "x");
    }

    // ── v0.6-specific: coordinate systems ────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void coordinateSystemsPresentInEntry() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

        assertNotNull(entry.coordinateSystems);
        assertEquals(1, entry.coordinateSystems.size());

        CoordinateSystem cs = entry.coordinateSystems.get(0);
        assertEquals("physical", cs.name);
        assertNotNull(cs.axes);
        assertEquals(2, cs.axes.size());
        assertEquals("y", cs.axes.get(0).name);
        assertEquals("x", cs.axes.get(1).name);
    }

    @Test
    @SuppressWarnings("unchecked")
    void datasetsPathsAndTransformations() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

        assertNotNull(entry.datasets);
        assertEquals(3, entry.datasets.size());
        assertEquals("s0", entry.datasets.get(0).path);
        assertEquals("s1", entry.datasets.get(1).path);
        assertEquals("s2", entry.datasets.get(2).path);

        dev.zarr.zarrjava.ome.v0_6.metadata.CoordinateTransformation ct =
                entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("scale", ct.type);
        assertEquals("s0", ct.input);
        assertEquals("physical", ct.output);
        assertNotNull(ct.scale);
        assertEquals(2, ct.scale.size());
        assertEquals(6.0, ct.scale.get(0), 1e-9);
        assertEquals(4.0, ct.scale.get(1), 1e-9);
    }

    @Test
    void unifiedInterfaceNodesAndPaths() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = image.getMultiscaleNode(0);

        assertEquals("multiscales", entry.name);
        assertEquals(3, entry.datasets.size());
        assertEquals("s0", entry.datasets.get(0).path);
        assertEquals("scale", entry.datasets.get(0).coordinateTransformations.get(0).type);
    }

    // ── 3D example ───────────────────────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void read3dAxesFromCoordinateSystems() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(V06_3D));
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_6.MultiscaleImage.class, image);

        dev.zarr.zarrjava.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

        assertEquals(3, entry.datasets.size());
        assertNotNull(entry.coordinateSystems);
        assertFalse(entry.coordinateSystems.isEmpty());

        List<dev.zarr.zarrjava.ome.metadata.Axis> axes = entry.coordinateSystems.get(0).axes;
        assertEquals(3, axes.size());
        assertEquals("z", axes.get(0).name);
        assertEquals("y", axes.get(1).name);
        assertEquals("x", axes.get(2).name);
    }

    @Test
    void read3dUnifiedAxisNames() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(V06_3D));
        List<String> axisNames = image.getAxisNames();
        assertEquals(Arrays.asList("z", "y", "x"), axisNames);
    }
}
