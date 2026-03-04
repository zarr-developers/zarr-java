package dev.zarr.zarrjava;

import dev.zarr.zarrjava.ome.MultiscaleImage;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.UnifiedMultiscaleNode;
import dev.zarr.zarrjava.ome.UnifiedSinglescaleNode;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrTest extends ZarrTest {

    private StoreHandle storeHandle(java.nio.file.Path path) throws Exception {
        return new FilesystemStore(path).resolve();
    }

    // ── v0.5 read tests ──────────────────────────────────────────────────────

    @Test
    void readV05_axesAndDatasets() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));

        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.class, image);

        UnifiedMultiscaleNode node = image.getMultiscaleNode(0);
        assertEquals("test_image", node.name);
        assertEquals(5, node.axes.size());
        assertEquals(2, node.nodes.size());

        List<String> axisNames = Arrays.asList("t", "c", "z", "y", "x");
        for (int i = 0; i < axisNames.size(); i++) {
            assertEquals(axisNames.get(i), node.axes.get(i).name);
        }

        List<String> axisTypes = Arrays.asList("time", "channel", "space", "space", "space");
        for (int i = 0; i < axisTypes.size(); i++) {
            assertEquals(axisTypes.get(i), node.axes.get(i).type);
        }

        UnifiedSinglescaleNode scaleNode0 = node.nodes.get(0);
        assertEquals("0", scaleNode0.path);
        assertEquals("scale", scaleNode0.coordinateTransformations.get(0).type);
    }

    @Test
    void readV04_axesAndDatasets() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.4")));

        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, image);

        UnifiedMultiscaleNode node = image.getMultiscaleNode(0);
        assertEquals("test_image", node.name);
        assertEquals(5, node.axes.size());
        assertEquals(2, node.nodes.size());

        List<String> axisNames = Arrays.asList("t", "c", "z", "y", "x");
        for (int i = 0; i < axisNames.size(); i++) {
            assertEquals(axisNames.get(i), node.axes.get(i).name);
        }

        List<String> axisTypes = Arrays.asList("time", "channel", "space", "space", "space");
        for (int i = 0; i < axisTypes.size(); i++) {
            assertEquals(axisTypes.get(i), node.axes.get(i).type);
        }

        UnifiedSinglescaleNode scaleNode0 = node.nodes.get(0);
        assertEquals("0", scaleNode0.path);
        assertEquals("scale", scaleNode0.coordinateTransformations.get(0).type);
    }

    @Test
    void readV05_getAxisNames() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), image.getAxisNames());
    }

    @Test
    void readV05_openScaleLevel() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));

        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        assertArrayEquals(new long[]{1, 2, 8, 16, 16}, level0.metadata().shape);

        dev.zarr.zarrjava.core.Array level1 = image.openScaleLevel(1);
        assertArrayEquals(new long[]{1, 2, 4, 8, 8}, level1.metadata().shape);
    }

    @Test
    void readV04_openScaleLevel() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.4")));

        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        assertArrayEquals(new long[]{1, 2, 8, 16, 16}, level0.metadata().shape);

        dev.zarr.zarrjava.core.Array level1 = image.openScaleLevel(1);
        assertArrayEquals(new long[]{1, 2, 4, 8, 8}, level1.metadata().shape);
    }

    @Test
    void readV05_scaleLevelCount() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));
        assertEquals(2, image.getScaleLevelCount());
    }

    // ── v0.5 write tests ─────────────────────────────────────────────────────

    @Test
    void writeV05_createAndReopen() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer")
        );
        MultiscalesEntry entry = new MultiscalesEntry(axes, Collections.emptyList());

        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_create"));
        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage created =
                dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.create(handle, entry);

        dev.zarr.zarrjava.v3.ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(16, 16)
                .withChunkShape(16, 16)
                .withDataType(DataType.FLOAT32)
                .build();
        List<CoordinateTransformation> transforms =
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(0.5, 0.5)));
        created.createScaleLevel("0", arrayMetadata, transforms);

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.class, reopened);
        assertEquals(Arrays.asList("z", "y"), reopened.getAxisNames());
        assertEquals(1, reopened.getScaleLevelCount());
        assertEquals("0", reopened.getMultiscaleNode(0).nodes.get(0).path);
    }

    // ── v0.4 write tests ─────────────────────────────────────────────────────

    @Test
    void writeV04_createAndReopen() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer")
        );
        MultiscalesEntry entry = new MultiscalesEntry(axes, Collections.emptyList());

        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_create"));
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.create(handle, entry);

        dev.zarr.zarrjava.v2.ArrayMetadata arrayMetadata = new dev.zarr.zarrjava.v2.ArrayMetadata(
                2,
                new long[]{16, 16},
                new int[]{16, 16},
                dev.zarr.zarrjava.v2.DataType.FLOAT32,
                0,
                dev.zarr.zarrjava.v2.Order.C,
                null,
                null,
                null
        );
        List<CoordinateTransformation> transforms =
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(0.5, 0.5)));
        created.createScaleLevel("0", arrayMetadata, transforms);

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, reopened);
        assertEquals(Arrays.asList("z", "y"), reopened.getAxisNames());
        assertEquals(1, reopened.getScaleLevelCount());
        assertEquals("0", reopened.getMultiscaleNode(0).nodes.get(0).path);
    }

    // ── typed metadata tests ─────────────────────────────────────────────────

    @Test
    void readV05_typedMetadata() throws Exception {
        MultiscalesMetadataImage<?> image =
                (MultiscalesMetadataImage<?>) MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));

        MultiscalesEntry entry = image.getMultiscalesEntry(0);
        assertEquals("test_image", entry.name);
        assertNull(entry.version);

        List<Double> expectedScale = Arrays.asList(1.0, 1.0, 0.5, 0.5, 0.5);
        assertEquals(expectedScale, entry.datasets.get(0).coordinateTransformations.get(0).scale);
    }

    @Test
    void readV04_entryHasVersion() throws Exception {
        MultiscalesMetadataImage<?> image =
                (MultiscalesMetadataImage<?>) MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.4")));

        MultiscalesEntry entry = image.getMultiscalesEntry(0);
        assertEquals("0.4", entry.version);
    }
}
