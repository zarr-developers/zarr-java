package dev.zarr.zarrjava.ome;

import dev.zarr.zarrjava.ome.metadata.Axis;
import dev.zarr.zarrjava.ome.metadata.CoordinateTransformation;
import dev.zarr.zarrjava.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.ome.metadata.WellRef;
import dev.zarr.zarrjava.ome.metadata.WellImage;
import dev.zarr.zarrjava.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrV04Test extends OmeZarrBaseTest {

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return storeHandle(TESTDATA.resolve("ome/v0.4"));
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class;
    }

    @Override
    int expectedScaleLevelCount() { return 2; }

    @Override
    long[] expectedLevel0Shape() { return new long[]{1, 2, 8, 16, 16}; }

    @Override
    List<String> expectedAxisNames() {
        return Arrays.asList("t", "c", "z", "y", "x");
    }

    // ── typed metadata ───────────────────────────────────────────────────────

    @Test
    void typedEntry_hasVersion() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        assertEquals("0.4", entry.version);
    }

    @Test
    void typedEntry_level0ScaleValues() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        List<Double> expected = Arrays.asList(1.0, 1.0, 0.5, 0.5, 0.5);
        assertEquals(expected, entry.datasets.get(0).coordinateTransformations.get(0).scale);
    }

    // ── omero + bioformats2raw ───────────────────────────────────────────────

    @Test
    void omero_channels() throws Exception {
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
        OmeroMetadata omero = image.getOmeroMetadata();
        assertNotNull(omero);
        assertEquals(2, omero.channels.size());
        assertEquals("DAPI", omero.channels.get(0).get("label"));
        assertEquals("color", omero.rdefs.get("model"));
    }

    @Test
    void bioformats2rawLayout_value() throws Exception {
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
        assertEquals(Integer.valueOf(3), image.getBioformats2rawLayout());
    }

    // ── labels ───────────────────────────────────────────────────────────────

    @Test
    void labels_list() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        assertEquals(Collections.singletonList("nuclei"), image.getLabels());
    }

    @Test
    void labels_openLabel() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        MultiscaleImage nuclei = image.openLabel("nuclei");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, nuclei);
        assertEquals(Arrays.asList("z", "y", "x"), nuclei.getAxisNames());
    }

    // ── HCS ──────────────────────────────────────────────────────────────────

    @Test
    void hcs_plate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.Plate.class, plate);
        PlateMetadata meta = plate.getPlateMetadata();
        assertEquals(2, meta.columns.size());
        assertEquals("A", meta.rows.get(0).name);
        assertEquals("A/1", meta.wells.get(0).path);
    }

    @Test
    void hcs_wellViaPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.Well.class, well);
        assertEquals(1, well.getWellMetadata().images.size());
        assertEquals("0", well.getWellMetadata().images.get(0).path);
    }

    @Test
    void hcs_fullNavigation() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        MultiscaleImage fov = plate.openWell("A/1").openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, fov);
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), fov.getAxisNames());
    }

    // ── write round-trips ────────────────────────────────────────────────────

    @Test
    void write_createAndReopen() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_create"));
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));
        created.createScaleLevel("0",
                new dev.zarr.zarrjava.v2.ArrayMetadata(2, new long[]{16, 16}, new int[]{16, 16},
                        dev.zarr.zarrjava.v2.DataType.FLOAT32, 0, dev.zarr.zarrjava.v2.Order.C, null, null, null),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(0.5, 0.5))));

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.class, reopened);
        assertEquals(Arrays.asList("z", "y"), reopened.getAxisNames());
        assertEquals(1, reopened.getScaleLevelCount());
    }

    @Test
    void write_omeroRoundTrip() throws Exception {
        List<Axis> axes = Arrays.asList(new Axis("z", "space", "micrometer"), new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_omero"));
        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));
        created.createScaleLevel("0",
                new dev.zarr.zarrjava.v2.ArrayMetadata(2, new long[]{16, 16}, new int[]{16, 16},
                        dev.zarr.zarrjava.v2.DataType.FLOAT32, 0, dev.zarr.zarrjava.v2.Order.C, null, null, null),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        java.util.Map<String, Object> ch = new java.util.HashMap<String, Object>();
        ch.put("label", "DAPI");
        java.util.Map<String, Object> rd = new java.util.HashMap<String, Object>();
        rd.put("model", "color");
        created.setOmeroMetadata(new OmeroMetadata(Collections.singletonList(ch), rd));

        dev.zarr.zarrjava.ome.v0_4.MultiscaleImage reopened =
                dev.zarr.zarrjava.ome.v0_4.MultiscaleImage.openMultiscaleImage(handle);
        OmeroMetadata got = reopened.getOmeroMetadata();
        assertNotNull(got);
        assertEquals("DAPI", got.channels.get(0).get("label"));
    }

    @Test
    void write_plateRoundTrip() throws Exception {
        PlateMetadata meta = new PlateMetadata(
                Arrays.asList(new NamedEntry("1"), new NamedEntry("2")),
                Arrays.asList(new NamedEntry("A"), new NamedEntry("B")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null);
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_plate"));
        dev.zarr.zarrjava.ome.v0_4.Plate.createPlate(handle, meta);
        Plate reopened = Plate.open(handle);
        assertEquals(2, reopened.getPlateMetadata().columns.size());
        assertEquals("A/1", reopened.getPlateMetadata().wells.get(0).path);
    }
}
