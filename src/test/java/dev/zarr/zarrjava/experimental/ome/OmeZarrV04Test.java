package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroChannel;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroRdefs;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroWindow;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OmeZarrV04Test extends OmeZarrBaseTest {
    private static final String UK1_S3_ENDPOINT = "https://uk1s3.embassy.ebi.ac.uk";

    private static StoreHandle publicIrdV04Store(String key) {
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create(UK1_S3_ENDPOINT))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
        return new S3Store(client, "idr", "zarr/v0.4/idr0048A").resolve(key);
    }

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return storeHandle(TESTDATA.resolve("ome/v0.4"));
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.class;
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
    void typedEntryHasVersion() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        assertEquals("0.4", entry.version);
    }

    @Test
    void typedEntryLevel0ScaleValues() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        List<Double> expected = Arrays.asList(1.0, 1.0, 0.5, 0.5, 0.5);
        assertEquals(expected, ((ScaleCoordinateTransformation) entry.datasets.get(0).coordinateTransformations.get(0)).scale);
    }

    // ── omero + bioformats2raw ───────────────────────────────────────────────

    @Test
    void omeroChannels() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
        OmeroMetadata omero = image.getOmeroMetadata();
        assertNotNull(omero);
        assertNull(omero.id);
        assertNull(omero.version);
        assertNull(omero.name);
        assertEquals(2, omero.channels.size());
        assertEquals("DAPI", omero.channels.get(0).label);
        assertEquals("color", omero.rdefs.model);
    }

    @Test
    void bioformats2rawLayoutValue() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
        assertEquals(Integer.valueOf(3), image.getBioformats2rawLayout());
    }

    // ── labels ───────────────────────────────────────────────────────────────

    @Test
    void labelsList() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        assertEquals(Collections.singletonList("nuclei"), image.getLabels());
    }

    @Test
    void labelsOpenLabel() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        MultiscaleImage nuclei = image.openLabel("nuclei");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.class, nuclei);
        assertEquals(Arrays.asList("z", "y", "x"), nuclei.getAxisNames());
    }

    // ── HCS ──────────────────────────────────────────────────────────────────

    @Test
    void hcsPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.Plate.class, plate);
        PlateMetadata meta = plate.getPlateMetadata();
        assertEquals(2, meta.columns.size());
        assertEquals("A", meta.rows.get(0).name);
        assertEquals("A/1", meta.wells.get(0).path);
    }

    @Test
    void hcsWellViaPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.Well.class, well);
        assertEquals(1, well.getWellMetadata().images.size());
        assertEquals("0", well.getWellMetadata().images.get(0).path);
    }

    @Test
    void hcsFullNavigation() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.4_hcs")));
        MultiscaleImage fov = plate.openWell("A/1").openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.class, fov);
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), fov.getAxisNames());
    }

    // ── write round-trips ────────────────────────────────────────────────────

    @Test
    void writeCreateAndReopen() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_create"));
        dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));
        created.createScaleLevel("0",
                new dev.zarr.zarrjava.v2.ArrayMetadata(2, new long[]{16, 16}, new int[]{16, 16},
                        dev.zarr.zarrjava.v2.DataType.FLOAT32, 0, dev.zarr.zarrjava.v2.Order.C, null, null, null),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(0.5, 0.5))));

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.class, reopened);
        assertEquals(Arrays.asList("z", "y"), reopened.getAxisNames());
        assertEquals(1, reopened.getScaleLevelCount());
    }

    @Test
    void writeOmeroRoundTrip() throws Exception {
        List<Axis> axes = Arrays.asList(new Axis("z", "space", "micrometer"), new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_omero"));
        dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage created =
                dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));
        created.createScaleLevel("0",
                new dev.zarr.zarrjava.v2.ArrayMetadata(2, new long[]{16, 16}, new int[]{16, 16},
                        dev.zarr.zarrjava.v2.DataType.FLOAT32, 0, dev.zarr.zarrjava.v2.Order.C, null, null, null),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        OmeroChannel ch = new OmeroChannel(
                true, 1.0, "0000FF", "linear", false, "DAPI",
                new OmeroWindow(0.0, 65535.0, 0.0, 1500.0));
        OmeroRdefs rd = new OmeroRdefs(0, 0, "color");
        created.setOmeroMetadata(new OmeroMetadata(1, "0.5", "example.tif", Collections.singletonList(ch), rd));

        dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage reopened =
                dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.openMultiscaleImage(handle);
        OmeroMetadata got = reopened.getOmeroMetadata();
        assertNotNull(got);
        assertEquals(Integer.valueOf(1), got.id);
        assertEquals("0.5", got.version);
        assertEquals("example.tif", got.name);
        assertEquals("DAPI", got.channels.get(0).label);
        assertEquals("color", got.rdefs.model);
    }

    @Test
    void writePlateRoundTrip() throws Exception {
        PlateMetadata meta = new PlateMetadata(
                Arrays.asList(new NamedEntry("1"), new NamedEntry("2")),
                Arrays.asList(new NamedEntry("A"), new NamedEntry("B")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null);
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v04_plate"));
        dev.zarr.zarrjava.experimental.ome.v0_4.Plate.createPlate(handle, meta);
        Plate reopened = Plate.open(handle);
        assertEquals(2, reopened.getPlateMetadata().columns.size());
        assertEquals("A/1", reopened.getPlateMetadata().wells.get(0).path);
    }

    @Test
    void openS3Idr0048AV04() throws Exception {
        StoreHandle handle = publicIrdV04Store("9846152.zarr");
        MultiscaleImage image = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_4.MultiscaleImage.class, image);
        assertEquals(9, image.getScaleLevelCount());

        MultiscalesMetadataImage<?> typed = (MultiscalesMetadataImage<?>) image;
        MultiscalesEntry entry = (MultiscalesEntry) typed.getMultiscalesEntry(0);
        assertEquals("0.4", entry.version);
        assertEquals(Arrays.asList("c", "z", "y", "x"), image.getAxisNames());
        assertEquals(9, entry.datasets.size());
        for (int i = 0; i < entry.datasets.size(); i++) {
            dev.zarr.zarrjava.experimental.ome.metadata.Dataset ds = entry.datasets.get(i);
            assertEquals(Integer.toString(i), ds.path);
            assertEquals("scale", ds.coordinateTransformations.get(0).type);
            assertEquals(4, ((ScaleCoordinateTransformation) ds.coordinateTransformations.get(0)).scale.size());
        }

        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        long[] shape = level0.metadata().shape;
        assertEquals(4, shape.length);
        // IDR sample catalog (idr0048A/9846152): X=19120, Y=13350, Z=91, C=3.
        assertArrayEquals(new long[]{3, 91, 13350, 19120}, shape);
        long[] offset = new long[shape.length];
        long[] readShape = new long[shape.length];
        Arrays.fill(readShape, 1L);
        assertEquals(1L, level0.read(offset, readShape).getSize());
        long[] shape1 = image.openScaleLevel(1).metadata().shape;
        for (int i = 0; i < shape.length; i++) {
            assertTrue(shape1[i] <= shape[i], "expected downsampled-or-equal shape at dim " + i);
        }
    }
}
