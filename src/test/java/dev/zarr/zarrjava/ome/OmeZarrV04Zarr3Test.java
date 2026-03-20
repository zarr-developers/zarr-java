package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class OmeZarrV04Zarr3Test extends OmeZarrBaseTest {

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return new FilesystemStore(TESTDATA.resolve("l4_sample").resolve("color")).resolve();
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.ome.v0_4_zarr3.MultiscaleImage.class;
    }

    @Override
    int expectedScaleLevelCount() {
        return 5;
    }

    @Override
    long[] expectedLevel0Shape() {
        return new long[]{1, 4096, 4096, 2048};
    }

    @Override
    java.util.List<String> expectedAxisNames() {
        return Arrays.asList("c", "x", "y", "z");
    }

    @Test
    void openL4SampleSegmentation() throws Exception {
        StoreHandle segmentation = new FilesystemStore(TESTDATA.resolve("l4_sample").resolve("segmentation")).resolve();
        MultiscaleImage image = MultiscaleImage.open(segmentation);

        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4_zarr3.MultiscaleImage.class, image);
        assertEquals(5, image.getScaleLevelCount());
        assertEquals(Arrays.asList("c", "x", "y", "z"), image.getAxisNames());
        assertArrayEquals(new long[]{1, 4096, 4096, 2048}, image.openScaleLevel(0).metadata().shape);
    }

    @Test
    void normalizedEntryCarriesCompatibilityVersion() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        dev.zarr.zarrjava.ome.metadata.MultiscalesEntry entry = image.getMultiscaleNode(0);
        assertEquals("0.4-zarr3", entry.version);
        assertEquals("1", entry.datasets.get(0).path);
        assertEquals("16-16-4", entry.datasets.get(entry.datasets.size() - 1).path);
        assertEquals("scale", entry.datasets.get(0).coordinateTransformations.get(0).type);
    }
}
