package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.CountingStore;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellImage;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.Group;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests that the OME-Zarr nodes read each node's metadata once, and that they walk down through the
 * consolidated metadata of an ancestor instead of reading every node from the store.
 */
public class OmeConsolidatedMetadataTest {

    private static final List<Axis> AXES =
            Arrays.asList(new Axis("y", "space", "micrometer"), new Axis("x", "space", "micrometer"));

    /**
     * Writes an OME-Zarr v0.5 plate with one well, one image and two scale levels:
     * <pre>
     * /            plate
     * /A/1         well
     * /A/1/0       multiscale image
     * /A/1/0/0     array
     * /A/1/0/1     array
     * </pre>
     */
    private static StoreHandle writePlate(CountingStore store) throws IOException, ZarrException {
        StoreHandle plateHandle = store.resolve();
        dev.zarr.zarrjava.experimental.ome.v0_5.Plate.createPlate(plateHandle, new PlateMetadata(
                Collections.singletonList(new NamedEntry("1")),
                Collections.singletonList(new NamedEntry("A")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null));
        dev.zarr.zarrjava.experimental.ome.v0_5.Well.createWell(
                plateHandle.resolve("A/1"),
                new WellMetadata(Collections.singletonList(new WellImage("0", null))));
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(
                        plateHandle.resolve("A/1").resolve("0"),
                        new MultiscalesEntry(AXES, Collections.emptyList()));
        image.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16)
                        .withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));
        image.createScaleLevel("1",
                Array.metadataBuilder().withShape(8, 8).withChunkShape(8, 8)
                        .withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(2.0, 2.0))));
        return plateHandle;
    }

    @Test
    public void testOpeningAPlateReadsItsMetadataOnce() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle plateHandle = writePlate(store);

        store.resetCounters();
        Plate plate = Plate.open(plateHandle);
        Assertions.assertNotNull(plate.getPlateMetadata());
        Assertions.assertEquals(1, store.readCalls.get(),
                "the version is picked from the metadata that was read, so zarr.json must not be"
                        + " probed and read again");
    }

    @Test
    public void testOpeningAnImageReadsItsMetadataOnce() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle plateHandle = writePlate(store);

        store.resetCounters();
        MultiscaleImage image = MultiscaleImage.open(plateHandle.resolve("A/1").resolve("0"));
        Assertions.assertEquals(2, image.getScaleLevelCount());
        Assertions.assertEquals(1, store.readCalls.get());
    }

    @Test
    public void testWalkingAConsolidatedPlateCostsNothingBelowTheRoot()
            throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle plateHandle = writePlate(store);
        Group.consolidateMetadata(plateHandle);

        store.resetCounters();
        Plate plate = Plate.open(plateHandle);
        Assertions.assertEquals(1, store.readCalls.get(), "only the plate's own metadata is read");

        store.resetCounters();
        Well well = plate.openWell("A/1");
        MultiscaleImage image = well.openImage("0");
        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        dev.zarr.zarrjava.core.Array level1 = image.openScaleLevel(1);

        Assertions.assertEquals(1, well.getWellMetadata().images.size());
        Assertions.assertEquals(Arrays.asList("y", "x"), image.getAxisNames());
        Assertions.assertArrayEquals(new long[]{16, 16}, level0.metadata().shape);
        Assertions.assertArrayEquals(new long[]{8, 8}, level1.metadata().shape);

        Assertions.assertEquals(0, store.readCalls.get(),
                "the whole walk below the plate must be answered from the consolidated metadata");
        Assertions.assertEquals(0, store.listCalls.get());
        Assertions.assertEquals(0, store.listChildrenCalls.get());
    }

    @Test
    public void testWalkingWithoutACacheStillWorks() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle plateHandle = writePlate(store);

        Plate plate = Plate.open(plateHandle);
        MultiscaleImage image = plate.openWell("A/1").openImage("0");
        Assertions.assertEquals(2, image.getScaleLevelCount());
        Assertions.assertArrayEquals(new long[]{16, 16}, image.openScaleLevel(0).metadata().shape);
    }

    @Test
    public void testMissingNodeBelowAConsolidatedPlateIsReported() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle plateHandle = writePlate(store);
        Group.consolidateMetadata(plateHandle);

        Plate plate = Plate.open(plateHandle);
        ZarrException exception =
                Assertions.assertThrows(ZarrException.class, () -> plate.openWell("B/2"));
        Assertions.assertTrue(exception.getMessage().contains("B/2"), exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("UseConsolidated.IGNORE"),
                "the message must point at the stale-cache case: " + exception.getMessage());
    }

    @Test
    public void testConsolidatedLabelsAreReadFromTheCache() throws IOException, ZarrException {
        CountingStore store = new CountingStore();
        StoreHandle imageHandle = store.resolve();
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(
                imageHandle, new MultiscalesEntry(AXES, Collections.emptyList()));
        Group.create(imageHandle.resolve("labels"),
                new dev.zarr.zarrjava.core.Attributes()
                        .set("labels", Collections.singletonList("nuclei")));
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage nuclei =
                dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(
                        imageHandle.resolve("labels").resolve("nuclei"),
                        new MultiscalesEntry(AXES, Collections.emptyList()));
        nuclei.createScaleLevel("0",
                Array.metadataBuilder().withShape(4, 4).withChunkShape(4, 4)
                        .withDataType(DataType.UINT8).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));
        Group.consolidateMetadata(imageHandle);

        MultiscaleImage image = MultiscaleImage.open(imageHandle);
        store.resetCounters();
        Assertions.assertEquals(Collections.singletonList("nuclei"), image.getLabels());
        MultiscaleImage label = image.openLabel("nuclei");
        Assertions.assertArrayEquals(new long[]{4, 4}, label.openScaleLevel(0).metadata().shape);
        Assertions.assertEquals(0, store.readCalls.get(),
                "the labels group and the label image must come from the consolidated metadata");
    }
}
