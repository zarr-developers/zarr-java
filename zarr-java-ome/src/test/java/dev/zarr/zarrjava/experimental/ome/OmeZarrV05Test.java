package dev.zarr.zarrjava.experimental.ome;

import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.experimental.ome.metadata.Axis;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.CoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.transform.ScaleCoordinateTransformation;
import dev.zarr.zarrjava.experimental.ome.metadata.MultiscalesEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.NamedEntry;
import dev.zarr.zarrjava.experimental.ome.metadata.OmeroMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.PlateMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellImage;
import dev.zarr.zarrjava.experimental.ome.metadata.WellMetadata;
import dev.zarr.zarrjava.experimental.ome.metadata.WellRef;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
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

public class OmeZarrV05Test extends OmeZarrBaseTest {
    private static final String UK1_S3_ENDPOINT = "https://uk1s3.embassy.ebi.ac.uk";

    private static StoreHandle publicIrdV05Store(String prefix, String key) {
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create(UK1_S3_ENDPOINT))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
        return new S3Store(client, "idr", prefix).resolve(key);
    }

    @Override
    StoreHandle imageStoreHandle() throws Exception {
        return storeHandle(TESTDATA.resolve("ome/v0.5"));
    }

    @Override
    Class<?> expectedConcreteClass() {
        return dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class;
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
    void typedEntryNoVersion() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        assertEquals("test_image", entry.name);
        assertNull(entry.version);
    }

    @Test
    void typedEntryLevel0ScaleValues() throws Exception {
        MultiscalesMetadataImage<?> image = (MultiscalesMetadataImage<?>) MultiscaleImage.open(imageStoreHandle());
        MultiscalesEntry entry = (MultiscalesEntry) image.getMultiscalesEntry(0);
        List<Double> expected = Arrays.asList(1.0, 1.0, 0.5, 0.5, 0.5);
        assertEquals(expected, ((ScaleCoordinateTransformation) entry.datasets.get(0).coordinateTransformations.get(0)).scale);
    }

    @Test
    void openScaleLevelLevel1HasExpectedShape() throws Exception {
        MultiscaleImage image = MultiscaleImage.open(imageStoreHandle());
        dev.zarr.zarrjava.core.Array level1 = image.openScaleLevel(1);
        assertArrayEquals(new long[]{1, 2, 4, 8, 8}, level1.metadata().shape);
    }

    // ── omero + bioformats2raw ───────────────────────────────────────────────

    @Test
    void omeroChannels() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
        OmeroMetadata omero = image.getOmeroMetadata();
        assertNotNull(omero);
        assertNull(omero.id);
        assertNull(omero.version);
        assertNull(omero.name);
        assertEquals(2, omero.channels.size());
        assertEquals("DAPI", omero.channels.get(0).label);
        assertEquals("GFP",  omero.channels.get(1).label);
        assertEquals("color", omero.rdefs.model);
    }

    @Test
    void bioformats2rawLayoutValue() throws Exception {
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage image =
                dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.openMultiscaleImage(imageStoreHandle());
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
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, nuclei);
        assertEquals(Arrays.asList("z", "y", "x"), nuclei.getAxisNames());
    }

    // ── HCS ──────────────────────────────────────────────────────────────────

    @Test
    void hcsPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.Plate.class, plate);
        PlateMetadata meta = plate.getPlateMetadata();
        assertEquals(2, meta.columns.size());
        assertEquals(2, meta.rows.size());
        assertEquals("A", meta.rows.get(0).name);
        assertEquals("1", meta.columns.get(0).name);
        assertEquals("A/1", meta.wells.get(0).path);
    }

    @Test
    void hcsWellViaPlate() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        Well well = plate.openWell("A/1");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.Well.class, well);
        assertEquals(1, well.getWellMetadata().images.size());
        assertEquals("0", well.getWellMetadata().images.get(0).path);
        assertEquals(Integer.valueOf(0), well.getWellMetadata().images.get(0).acquisition);
    }

    @Test
    void hcsFullNavigation() throws Exception {
        Plate plate = Plate.open(storeHandle(TESTDATA.resolve("ome/v0.5_hcs")));
        MultiscaleImage fov = plate.openWell("A/1").openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, fov);
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), fov.getAxisNames());
    }

    // ── write round-trips ────────────────────────────────────────────────────

    @Test
    void writeCreateAndReopen() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_create"));
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage created =
                dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));
        created.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16).withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(0.5, 0.5))));

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, reopened);
        assertEquals(Arrays.asList("z", "y"), reopened.getAxisNames());
        assertEquals(1, reopened.getScaleLevelCount());
        assertEquals("0", reopened.getMultiscaleNode(0).datasets.get(0).path);
    }

    @Test
    void writeLabelsRoundTrip() throws Exception {
        List<Axis> axes = Arrays.asList(
                new Axis("z", "space", "micrometer"),
                new Axis("y", "space", "micrometer"));
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_labels"));
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(handle, new MultiscalesEntry(axes, Collections.emptyList()));

        Attributes labelsAttrs = new Attributes();
        labelsAttrs.put("labels", Arrays.asList("nuclei"));
        dev.zarr.zarrjava.v3.Group.create(handle.resolve("labels"), labelsAttrs);

        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage nuclei = dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(
                handle.resolve("labels").resolve("nuclei"), new MultiscalesEntry(axes, Collections.emptyList()));
        nuclei.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16).withDataType(DataType.UINT8).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        MultiscaleImage reopened = MultiscaleImage.open(handle);
        assertEquals(Collections.singletonList("nuclei"), reopened.getLabels());
        assertEquals(Arrays.asList("z", "y"), reopened.openLabel("nuclei").getAxisNames());
    }

    @Test
    void writePlateRoundTrip() throws Exception {
        PlateMetadata meta = new PlateMetadata(
                Arrays.asList(new NamedEntry("1"), new NamedEntry("2")),
                Arrays.asList(new NamedEntry("A"), new NamedEntry("B")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null);
        StoreHandle handle = storeHandle(TESTOUTPUT.resolve("ome_v05_plate"));
        dev.zarr.zarrjava.experimental.ome.v0_5.Plate.createPlate(handle, meta);
        Plate reopened = Plate.open(handle);
        assertEquals(2, reopened.getPlateMetadata().columns.size());
        assertEquals("A", reopened.getPlateMetadata().rows.get(0).name);
        assertEquals("A/1", reopened.getPlateMetadata().wells.get(0).path);
    }

    @Test
    void writeHcsFullIntegration() throws Exception {
        StoreHandle plateHandle = storeHandle(TESTOUTPUT.resolve("ome_v05_hcs_full"));
        dev.zarr.zarrjava.experimental.ome.v0_5.Plate.createPlate(plateHandle, new PlateMetadata(
                Collections.singletonList(new NamedEntry("1")),
                Collections.singletonList(new NamedEntry("A")),
                Collections.singletonList(new WellRef("A/1", 0, 0)),
                null, null, null, null));
        dev.zarr.zarrjava.experimental.ome.v0_5.Well.createWell(
                plateHandle.resolve("A/1"),
                new WellMetadata(Collections.singletonList(new WellImage("0", null))));

        List<Axis> axes = Arrays.asList(new Axis("z", "space", "micrometer"), new Axis("y", "space", "micrometer"));
        dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage fov = dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.create(
                plateHandle.resolve("A/1").resolve("0"), new MultiscalesEntry(axes, Collections.emptyList()));
        fov.createScaleLevel("0",
                Array.metadataBuilder().withShape(16, 16).withChunkShape(16, 16).withDataType(DataType.FLOAT32).build(),
                Collections.singletonList(CoordinateTransformation.scale(Arrays.asList(1.0, 1.0))));

        MultiscaleImage image = Plate.open(plateHandle).openWell("A/1").openImage("0");
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, image);
        assertEquals(Arrays.asList("z", "y"), image.getAxisNames());
    }

    @Test
    void openS3Idr0083V05() throws Exception {
        StoreHandle handle = publicIrdV05Store("zarr/v0.5/idr0083", "9822152.zarr");
        MultiscaleImage image = MultiscaleImage.open(handle);
        assertInstanceOf(dev.zarr.zarrjava.experimental.ome.v0_5.MultiscaleImage.class, image);
        assertEquals(11, image.getScaleLevelCount());

        MultiscalesMetadataImage<?> typed = (MultiscalesMetadataImage<?>) image;
        MultiscalesEntry entry = (MultiscalesEntry) typed.getMultiscalesEntry(0);
        assertNotNull(entry);
        assertNull(entry.version); // v0.5 keeps version at ome.version, not per multiscales entry
        assertEquals(Arrays.asList("t", "c", "z", "y", "x"), image.getAxisNames());
        assertEquals(11, entry.datasets.size());
        for (int i = 0; i < entry.datasets.size(); i++) {
            dev.zarr.zarrjava.experimental.ome.metadata.Dataset ds = entry.datasets.get(i);
            assertEquals(Integer.toString(i), ds.path);
            assertEquals("scale", ds.coordinateTransformations.get(0).type);
            assertEquals(5, ((ScaleCoordinateTransformation) ds.coordinateTransformations.get(0)).scale.size());
        }

        dev.zarr.zarrjava.core.Array level0 = image.openScaleLevel(0);
        long[] shape = level0.metadata().shape;
        assertEquals(5, shape.length);
        // IDR sample catalog (idr0083/9822152): X=144384, Y=93184, Z=1, C=1, T=1.
        assertArrayEquals(new long[]{1, 1, 1, 93184, 144384}, shape);
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
