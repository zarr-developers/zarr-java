package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.Dataset;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OmeZarrUserGuideExamplesTest extends ZarrTest {

    private StoreHandle storeHandle(java.nio.file.Path path) throws Exception {
        return new FilesystemStore(path).resolve();
    }

    @Test
    void userGuideReadingExampleWorks() throws Exception {
        StoreHandle imageHandle = storeHandle(TESTDATA.resolve("ome/v0.5"));
        MultiscaleImage image = MultiscaleImage.open(imageHandle);

        int scaleCount = image.getScaleLevelCount();
        List<String> axisNames = image.getAxisNames();
        dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry entry0 = image.getMultiscaleNode(0);

        dev.zarr.zarrjava.core.Array s0 = image.openScaleLevel(0);
        ucar.ma2.Array full = s0.read();
        ucar.ma2.Array subset = s0.read(new long[]{0, 0, 0, 0, 0}, new long[]{1, 1, 4, 8, 8});

        assertTrue(scaleCount > 0);
        assertFalse(axisNames.isEmpty());
        assertNotNull(entry0);
        assertArrayEquals(new int[]{1, 2, 8, 16, 16}, full.getShape());
        assertArrayEquals(new int[]{1, 1, 4, 8, 8}, subset.getShape());

        List<String> labels = image.getLabels();
        assertEquals(Collections.singletonList("nuclei"), labels);
        MultiscaleImage labelImage = image.openLabel(labels.get(0));
        assertEquals(Arrays.asList("z", "y", "x"), labelImage.getAxisNames());

        StoreHandle plateHandle = storeHandle(TESTDATA.resolve("ome/v0.5_hcs"));
        Plate plate = Plate.open(plateHandle);
        Well well = plate.openWell("A/1");
        MultiscaleImage wellImage = well.openImage("0");
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), wellImage.getAxisNames());
    }

    @Test
    void userGuideWritingExampleWorks() throws Exception {
        StoreHandle out = storeHandle(TESTOUTPUT.resolve("ome_userguide_v05.zarr"));
        MultiscalesEntry ms = new MultiscalesEntry(
                Arrays.asList(new Axis("y", "space", "micrometer"), new Axis("x", "space", "micrometer")),
                Collections.<Dataset>emptyList());
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage written = dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(out, ms);

        written.createScaleLevel(
                "s0",
                Array.metadataBuilder()
                        .withShape(1024, 1024)
                        .withChunkShape(256, 256)
                        .withDataType(DataType.UINT16)
                        .build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));
        written.createScaleLevel(
                "s1",
                Array.metadataBuilder()
                        .withShape(512, 512)
                        .withChunkShape(256, 256)
                        .withDataType(DataType.UINT16)
                        .build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(2.0, 2.0))));

        MultiscaleImage reopened = MultiscaleImage.open(out);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, reopened);
        assertEquals(2, reopened.getScaleLevelCount());
        assertEquals("s0", reopened.getMultiscaleNode(0).datasets.get(0).path);
        assertEquals("s1", reopened.getMultiscaleNode(0).datasets.get(1).path);
        assertEquals(Arrays.asList(2.0, 2.0),
                ((ScaleCoordinateTransformation) reopened.getMultiscaleNode(0).datasets.get(1).coordinateTransformations.get(0)).scale);
    }
}
