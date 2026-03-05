package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.ome.MultiscaleImage;
import dev.zarr.zarrjava.ome.MultiscalesMetadataImage;
import dev.zarr.zarrjava.ome.Plate;
import dev.zarr.zarrjava.ome.UnifiedMultiscaleNode;
import dev.zarr.zarrjava.ome.UnifiedSinglescaleNode;
import dev.zarr.zarrjava.ome.Well;
import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.ome.metadata.WellImage;
import dev.zarr.zarrjava.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.ome.metadata.WellRef;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // ── Omero + bioformats2raw.layout (read from testdata) ──────────────────

    @Test
    void readV05_omero() throws Exception {
        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.openMultiscaleImage(
                        storeHandle(TESTDATA.resolve("ome/v0.5")));

        OmeroMetadata omero = image.getOmeroMetadata();
        assertNotNull(omero);
        assertEquals(2, omero.channels.size());
        assertEquals("DAPI", omero.channels.get(0).get("label"));
        assertEquals("GFP",  omero.channels.get(1).get("label"));
        assertEquals("color", omero.rdefs.get("model"));
    }

    @Test
    void readV04_omero() throws Exception {
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(
                        storeHandle(TESTDATA.resolve("ome/v0.4")));

        OmeroMetadata omero = image.getOmeroMetadata();
        assertNotNull(omero);
        assertEquals(2, omero.channels.size());
        assertEquals("DAPI", omero.channels.get(0).get("label"));
        assertEquals("color", omero.rdefs.get("model"));
    }

    @Test
    void readV05_bioformats2rawLayout() throws Exception {
        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.openMultiscaleImage(
                        storeHandle(TESTDATA.resolve("ome/v0.5")));
        assertEquals(Integer.valueOf(3), image.getBioformats2rawLayout());
    }

    @Test
    void readV04_bioformats2rawLayout() throws Exception {
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(
                        storeHandle(TESTDATA.resolve("ome/v0.4")));
        assertEquals(Integer.valueOf(3), image.getBioformats2rawLayout());
    }

    // ── Labels (read from testdata) ──────────────────────────────────────────

    @Test
    void readV05_labels() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.5")));

        List<String> labels = image.getLabels();
        assertEquals(Collections.singletonList("nuclei"), labels);

        MultiscaleImage nuclei = image.openLabel("nuclei");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.class, nuclei);
        assertEquals(Arrays.asList("z", "y", "x"), nuclei.getAxisNames());
    }

    @Test
    void readV04_labels() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(TESTDATA.resolve("ome/v0.4")));

        List<String> labels = image.getLabels();
        assertEquals(Collections.singletonList("nuclei"), labels);

        MultiscaleImage nuclei = image.openLabel("nuclei");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, nuclei);
        assertEquals(Arrays.asList("z", "y", "x"), nuclei.getAxisNames());
    }

    // ── HCS Plate (read from testdata) ───────────────────────────────────────

    @Test
    void readV05_plate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.Plate.class, plate);

        PlateMetadata meta = plate.getPlateMetadata();
        assertEquals(2, meta.columns.size());
        assertEquals(2, meta.rows.size());
        assertEquals("A", meta.rows.get(0).name);
        assertEquals("1", meta.columns.get(0).name);
        assertEquals("A/1", meta.wells.get(0).path);
    }

    @Test
    void readV04_plate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.Plate.class, plate);

        PlateMetadata meta = plate.getPlateMetadata();
        assertEquals(2, meta.columns.size());
        assertEquals("A", meta.rows.get(0).name);
        assertEquals("A/1", meta.wells.get(0).path);
    }

    @Test
    void readV05_wellViaPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.Well.class, well);

        assertEquals(1, well.getWellMetadata().images.size());
        assertEquals("0", well.getWellMetadata().images.get(0).path);
        assertEquals(Integer.valueOf(0), well.getWellMetadata().images.get(0).acquisition);
    }

    @Test
    void readV04_wellViaPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.Well.class, well);

        assertEquals(1, well.getWellMetadata().images.size());
        assertEquals("0", well.getWellMetadata().images.get(0).path);
    }

    @Test
    void readV05_hcsFullNavigation() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        Well well = plate.openWell("A/1");
        MultiscaleImage fov = well.openImage("0");

        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.class, fov);
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), fov.getAxisNames());
    }

    @Test
    void readV04_hcsFullNavigation() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        Well well = plate.openWell("A/1");
        MultiscaleImage fov = well.openImage("0");

        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, fov);
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), fov.getAxisNames());
    }

    // ── Omero write round-trip (v0.4) ────────────────────────────────────────

    @Test
    void writeV04_omeroRoundTrip() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer")
        );
        MultiscalesEntry entry = new MultiscalesEntry(axes, Collections.emptyList());

        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_omero"));
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.create(handle, entry);

        dev.zarr.zarrjava.v2.ArrayMetadata arrayMetadata = new dev.zarr.zarrjava.v2.ArrayMetadata(
                2, new long[]{16, 16}, new int[]{16, 16},
                dev.zarr.zarrjava.v2.DataType.FLOAT32, 0,
                dev.zarr.zarrjava.v2.Order.C, null, null, null);
        created.createScaleLevel("0", arrayMetadata,
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        Map<String, Object> channelMap = new HashMap<String, Object>();
        channelMap.put("label", "DAPI");
        channelMap.put("color", "0000FF");
        Map<String, Object> rdefsMap = new HashMap<String, Object>();
        rdefsMap.put("model", "color");
        OmeroMetadata omero = new OmeroMetadata(Collections.singletonList(channelMap), rdefsMap);
        created.setOmeroMetadata(omero);

        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage reopened =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(handle);
        OmeroMetadata got = reopened.getOmeroMetadata();
        assertNotNull(got);
        assertEquals("DAPI", got.channels.get(0).get("label"));
        assertEquals("color", got.rdefs.get("model"));
    }

    // ── Labels write round-trip (v0.5) ───────────────────────────────────────

    @Test
    void writeV05_labelsRoundTrip() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer")
        );
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_labels"));
        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage parent =
                dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));

        Attributes labelsAttrs = new Attributes();
        labelsAttrs.put("labels", Arrays.asList("nuclei"));
        dev.zarr.zarrjava.v3.Group.create(handle.resolve("labels"), labelsAttrs);

        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage nuclei =
                dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.create(
                        handle.resolve("labels").resolve("nuclei"),
                        new MultiscalesEntry(axes, Collections.emptyList()));
        nuclei.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16).withDataType(DataType.UINT8).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertEquals(Collections.singletonList("nuclei"), reopened.getLabels());
        assertEquals(Arrays.asList("z", "y"), reopened.openLabel("nuclei").getAxisNames());
    }

    // ── HCS write round-trips ────────────────────────────────────────────────

    @Test
    void writeV05_plateRoundTrip() throws Exception {
        PlateMetadata plateMetadata = new PlateMetadata(
                Arrays.asList(new NamedEntry("1"), new NamedEntry("2")),
                Arrays.asList(new NamedEntry("A"), new NamedEntry("B")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null);

        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_plate"));
        dev.zarr.zarrjava.ome.v0_5.Plate.createPlate(handle, plateMetadata);

        Plate reopened = Plate.open(handle);
        assertEquals(2, reopened.getPlateMetadata().columns.size());
        assertEquals("A", reopened.getPlateMetadata().rows.get(0).name);
        assertEquals("A/1", reopened.getPlateMetadata().wells.get(0).path);
    }

    @Test
    void writeV04_plateRoundTrip() throws Exception {
        PlateMetadata plateMetadata = new PlateMetadata(
                Arrays.asList(new NamedEntry("1"), new NamedEntry("2")),
                Arrays.asList(new NamedEntry("A"), new NamedEntry("B")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null);

        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_plate"));
        dev.zarr.zarrjava.ome.v0_4.Plate.createPlate(handle, plateMetadata);

        Plate reopened = Plate.open(handle);
        assertEquals(2, reopened.getPlateMetadata().columns.size());
        assertEquals("A/1", reopened.getPlateMetadata().wells.get(0).path);
    }

    @Test
    void writeV05_hcsFullIntegration() throws Exception {
        StoreHandle plateHandle = storeHandle(TESTOUTPUT.resolve("ome_v05_hcs_full"));

        dev.zarr.zarrjava.ome.v0_5.Plate.createPlate(plateHandle, new PlateMetadata(
                Collections.singletonList(new NamedEntry("1")),
                Collections.singletonList(new NamedEntry("A")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null));

        dev.zarr.zarrjava.ome.v0_5.Well.createWell(
                plateHandle.resolve("A/1"),
                new WellMetadata(Collections.singletonList(new WellImage("0", null))));

        List<Axis> axes = Arrays.asList(new Axis("z", "space", "micrometer"), new Axis("y", "space", "micrometer"));
        dev.zarr.zarrjava.ome.v0_5.MultiscaleImage fov = dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.create(
                plateHandle.resolve("A/1").resolve("0"),
                new MultiscalesEntry(axes, Collections.emptyList()));
        fov.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16).withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        MultiscaleImage image = Plate.open(plateHandle).openWell("A/1").openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_5.MultiscaleImage.class, image);
        assertEquals(Arrays.asList("z", "y"), image.getAxisNames());
    }
}
