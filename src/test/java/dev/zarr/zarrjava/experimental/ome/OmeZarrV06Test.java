package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroChannel;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroRdefs;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroWindow;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellImage;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.CoordinateSystem;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrV06Test extends OmeZarrBaseTest {

    private static final java.nio.file.Path V06_2D =
            TESTDATA.resolve("ome/v0.6/examples/2d/basic/scale_multiscale.zarr");
    private static final java.nio.file.Path V06_3D =
            TESTDATA.resolve("ome/v0.6/examples/3d/basic/scale_multiscale.zarr");
    private static final java.nio.file.Path V06_HUMAN_ORGAN_ATLAS_OVERVIEW =
            TESTDATA.resolve("ome/v0.6/examples/user_stories/human_organ_atlas.zarr/overview.ome.zarr");

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return storeHandle(V06_2D);
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class;
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
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

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
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

        assertNotNull(entry.datasets);
        assertEquals(3, entry.datasets.size());
        assertEquals("s0", entry.datasets.get(0).path);
        assertEquals("s1", entry.datasets.get(1).path);
        assertEquals("s2", entry.datasets.get(2).path);

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation ct =
                entry.datasets.get(0).coordinateTransformations.get(0);
        assertEquals("scale", ct.getType());
        assertEquals("s0", ct.getInput());
        assertEquals("physical", ct.getOutput());
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation.class, ct);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation scaleCt =
                (dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.ScaleCoordinateTransformation) ct;
        assertNotNull(scaleCt.scale);
        assertEquals(2, scaleCt.scale.size());
        assertEquals(6.0, scaleCt.scale.get(0), 1e-9);
        assertEquals(4.0, scaleCt.scale.get(1), 1e-9);
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
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class, image);

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage v06Image =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry = v06Image.getMultiscalesEntry(0);

        assertEquals(3, entry.datasets.size());
        assertNotNull(entry.coordinateSystems);
        assertFalse(entry.coordinateSystems.isEmpty());

        List<dev.zarr.zarrjava.experimental.ome.metadata.Axis> axes = entry.coordinateSystems.get(0).axes;
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

    @Test
    void readHumanOrganAtlasOverview() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(storeHandle(V06_HUMAN_ORGAN_ATLAS_OVERVIEW));
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class, image);

        assertEquals(2, image.getScaleLevelCount());
        assertEquals(Arrays.asList("x", "y", "z"), image.getAxisNames());
        assertEquals(Arrays.asList("0", "1"), Arrays.asList(
                image.getMultiscaleNode(0).datasets.get(0).path,
                image.getMultiscaleNode(0).datasets.get(1).path));

        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        assertArrayEquals(new long[]{8308, 8308, 9564}, level0.metadata().shape);

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage typed = (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) image;
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry entry = typed.getMultiscalesEntry(0);
        assertNotNull(entry.coordinateSystems);
        assertEquals(2, entry.coordinateSystems.size());
        assertEquals("physical", entry.coordinateSystems.get(0).name);
        assertEquals("anatomical", entry.coordinateSystems.get(1).name);
        assertEquals("sequence", entry.datasets.get(0).coordinateTransformations.get(0).getType());
    }

    @Test
    void readV06WithOmeroMetadata() throws Exception {
        java.nio.file.Path out = TESTOUTPUT.resolve("ome_v06_with_omero");
        StoreHandle outHandle = storeHandle(out);
        java.util.List<dev.zarr.zarrjava.experimental.ome.metadata.Axis> axes = Arrays.asList(
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("y", "space", "micrometer"),
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("x", "space", "micrometer"));
        java.util.List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset> datasets =
                java.util.Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset(
                        "s0",
                        java.util.Collections.singletonList(
                                dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation.scale(
                                        Arrays.asList(1.0, 1.0), "s0", "physical"))));
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry ms =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                        null,
                        datasets,
                        null,
                        java.util.Collections.singletonList(new CoordinateSystem("physical", axes)),
                        "multiscales",
                        null,
                        null);

        OmeroChannel ch = new OmeroChannel(
                true,
                1.0,
                "0000FF",
                "linear",
                false,
                "LaminB1",
                new OmeroWindow(0.0, 65535.0, 0.0, 1500.0));
        OmeroChannel ch2 = new OmeroChannel(
                true,
                1.0,
                "00FF00",
                "linear",
                false,
                "Actin",
                new OmeroWindow(10.0, 4096.0, 50.0, 2000.0));
        dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata omero =
                new dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata(
                        1,
                        "0.5",
                        "example.tif",
                        Arrays.asList(ch, ch2),
                        new OmeroRdefs(0, 0, "color"));

        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata ome =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata("0.6", java.util.Collections.singletonList(ms), omero);
        dev.zarr.zarrjava.v3.Group.create(outHandle, dev.zarr.zarrjava.experimental.ome.OmeV3Group.omeAttributes(ome));
        dev.zarr.zarrjava.v3.Array.create(
                outHandle.resolve("s0"),
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16)
                        .withChunkShape(8, 8)
                        .withDataType(dev.zarr.zarrjava.v3.DataType.UINT16)
                        .build());

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage read =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) MultiscaleImage.open(outHandle);
        assertNotNull(read.getOmeroMetadata());
        assertEquals(Integer.valueOf(1), read.getOmeroMetadata().id);
        assertEquals("0.5", read.getOmeroMetadata().version);
        assertEquals("example.tif", read.getOmeroMetadata().name);
        assertEquals(2, read.getOmeroMetadata().channels.size());
        assertEquals("LaminB1", read.getOmeroMetadata().channels.get(0).label);
        assertEquals("Actin", read.getOmeroMetadata().channels.get(1).label);
        assertEquals("color", read.getOmeroMetadata().rdefs.model);
    }

    @Test
    void readV06WithBioformats2rawLayout() throws Exception {
        java.nio.file.Path out = TESTOUTPUT.resolve("ome_v06_with_layout");
        StoreHandle outHandle = storeHandle(out);

        java.util.List<dev.zarr.zarrjava.experimental.ome.metadata.Axis> axes = Arrays.asList(
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("y", "space", "micrometer"),
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("x", "space", "micrometer"));
        java.util.List<dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset> datasets =
                java.util.Collections.singletonList(new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.Dataset(
                        "s0",
                        java.util.Collections.singletonList(
                                dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation.scale(
                                        Arrays.asList(1.0, 1.0), "s0", "physical"))));
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry ms =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                        null,
                        datasets,
                        null,
                        java.util.Collections.singletonList(new CoordinateSystem("physical", axes)),
                        "multiscales",
                        null,
                        null);
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata ome =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.OmeMetadata(
                        "0.6",
                        java.util.Collections.singletonList(ms),
                        null,
                        7,
                        null,
                        null,
                        null);
        dev.zarr.zarrjava.v3.Group.create(outHandle, dev.zarr.zarrjava.experimental.ome.OmeV3Group.omeAttributes(ome));
        dev.zarr.zarrjava.v3.Array.create(
                outHandle.resolve("s0"),
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16)
                        .withChunkShape(8, 8)
                        .withDataType(dev.zarr.zarrjava.v3.DataType.UINT16)
                        .build());

        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage read =
                (dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage) MultiscaleImage.open(outHandle);
        assertEquals(Integer.valueOf(7), read.getBioformats2rawLayout());
    }

    @Test
    void hcsV06PlateWellDispatchAndNavigation() throws Exception {
        StoreHandle plateHandle = storeHandle(TESTOUTPUT.resolve("ome_v06_hcs_full"));
        dev.zarr.zarrjava.experimental.ome.v0_6.Plate.createPlate(plateHandle, new PlateMetadata(
                Collections.singletonList(new NamedEntry("1")),
                Collections.singletonList(new NamedEntry("A")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null));
        dev.zarr.zarrjava.experimental.ome.v0_6.Well.createWell(
                plateHandle.resolve("A/1"),
                new WellMetadata(Collections.singletonList(new WellImage("0", null))));

        List<dev.zarr.zarrjava.experimental.ome.metadata.Axis> axes = Arrays.asList(
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("y", "space", "micrometer"),
                new dev.zarr.zarrjava.experimental.ome.metadata.Axis("x", "space", "micrometer"));
        dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry ms =
                new dev.zarr.zarrjava.experimental.ome.v0_6.metadata.MultiscalesEntry(
                        null,
                        Collections.emptyList(),
                        null,
                        Collections.singletonList(new CoordinateSystem("physical", axes)),
                        "multiscales",
                        null,
                        null);
        dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage fov = dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.create(
                plateHandle.resolve("A/1").resolve("0"), ms);
        fov.createScaleLevel(
                "s0",
                dev.zarr.zarrjava.v3.Array.metadataBuilder()
                        .withShape(16, 16)
                        .withChunkShape(8, 8)
                        .withDataType(dev.zarr.zarrjava.v3.DataType.FLOAT32)
                        .build(),
                Collections.singletonList(dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation.scale(
                        Arrays.asList(1.0, 1.0))));

        Plate plate = Plate.open(plateHandle);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.Plate.class, plate);
        assertEquals("A/1", plate.getPlateMetadata().wells.get(0).path);
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.Well.class, well);
        assertEquals("0", well.getWellMetadata().images.get(0).path);
        MultiscaleImage image = well.openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_6.MultiscaleImage.class, image);
        assertEquals(Collections.singletonList("s0"),
                Collections.singletonList(image.getMultiscaleNode(0).datasets.get(0).path));
        assertEquals(Arrays.asList("y", "x"), image.getAxisNames());
    }
}
